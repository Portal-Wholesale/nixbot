"""Build machines: a cached, classified view of the Tribuchet hub.

The hub serves a read-only JSON snapshot at `GET /status` on its
loopback metrics listener (see Tribuchet's README). It reports facts
only: connected workers, current assignments, the queue, and workers
that departed within its short `worker_grace`. Nixbot owns the rest:
a poller caches the last good snapshot, remembers departures for
longer than the hub does by diffing consecutive snapshots, and
classifies workers as static (on the configured expected list) or
burst.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Mapping

    from .config import MachinesConfig
    from .web.logs import LogRegistry, RunningDrv
    from .web.queries import WebQueries

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5.0
# How long a departed worker stays on the page. The hub forgets it
# after its worker_grace (~30s).
DEPARTURE_WINDOW = 10 * 60
REQUEST_TIMEOUT = 5.0


class _HubModel(BaseModel):
    # Newer hubs may add fields.
    model_config = ConfigDict(extra="ignore", frozen=True)


class HubWorker(_HubModel):
    name: str
    systems: list[str] = []
    max_jobs: int
    free_slots: int
    connected_at: int
    last_heartbeat_at: int | None = None


class HubAssignment(_HubModel):
    job_id: str
    worker: str
    system: str
    name: str = ""
    outputs: dict[str, str] = {}
    started_at: int


class HubQueued(_HubModel):
    job_id: str
    system: str
    name: str = ""
    outputs: dict[str, str] = {}
    enqueued_at: int


class HubDeparted(_HubModel):
    name: str
    systems: list[str] = []
    departed_at: int


class HubSnapshot(_HubModel):
    """The hub's `/status` body. Times are Unix seconds."""

    generated_at: int
    queue_depth: int = 0
    workers: list[HubWorker] = []
    assignments: list[HubAssignment] = []
    queued: list[HubQueued] = []
    departed: list[HubDeparted] = []

    def sessions_by_name(self) -> dict[str, HubWorker]:
        """One session per worker name. During an overlapping reconnect
        the hub briefly lists the same name twice; the newest session
        is the one that stays."""
        by_name: dict[str, HubWorker] = {}
        for worker in self.workers:
            current = by_name.get(worker.name)
            if current is None or worker.connected_at >= current.connected_at:
                by_name[worker.name] = worker
        return by_name


WorkerKind = Literal["static", "burst"]
# unknown: no snapshot has been fetched yet, so nothing is known about
# an expected worker. Never inferred from a failed poll.
WorkerState = Literal["up", "down", "departed", "unknown"]


@dataclass(frozen=True)
class Departure:
    name: str
    systems: tuple[str, ...]
    departed_at: float


@dataclass(frozen=True)
class WorkerView:
    name: str
    kind: WorkerKind
    state: WorkerState
    # The live session for up workers.
    session: HubWorker | None = None
    # Departure time for departed workers, and for down static workers
    # that were seen leaving.
    departed_at: float | None = None


class MachinesMonitor:
    """Polls the hub every few seconds and keeps the last good snapshot.

    A failed poll records the error and keeps the previous snapshot and
    departure memory as they were: an unreachable hub says nothing
    about the workers.
    """

    def __init__(
        self,
        config: MachinesConfig,
        *,
        http: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
        interval: float = POLL_INTERVAL,
        departure_window: float = DEPARTURE_WINDOW,
    ) -> None:
        self.status_url = config.status_url
        self.expected_workers = tuple(dict.fromkeys(config.expected_workers))
        self._http = http
        self._owns_http = http is None
        self._clock = clock
        self.interval = interval
        self.departure_window = departure_window
        self.snapshot: HubSnapshot | None = None
        # Wall-clock time of the last good poll.
        self.fetched_at: float | None = None
        # Last poll failure; cleared by the next good poll.
        self.error: str | None = None
        self.error_at: float | None = None
        self._departures: dict[str, Departure] = {}
        self._task: asyncio.Task[None] | None = None
        self.drv_outputs = DrvOutputs()

    @property
    def reachable(self) -> bool:
        return self.snapshot is not None and self.error is None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def poll_once(self) -> bool:
        """Fetch one snapshot. Returns whether it succeeded."""
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
        try:
            response = await self._http.get(self.status_url)
            response.raise_for_status()
            snapshot = HubSnapshot.model_validate_json(response.content)
        except (httpx.HTTPError, ValidationError) as e:
            self.error = _describe(e)
            self.error_at = self._clock()
            logger.warning(
                "hub status poll failed",
                extra={"url": self.status_url, "error": self.error},
            )
            return False
        self._apply(snapshot)
        return True

    def _apply(self, snapshot: HubSnapshot) -> None:
        now = self._clock()
        connected = snapshot.sessions_by_name()
        if self.snapshot is not None:
            for name, worker in self.snapshot.sessions_by_name().items():
                if name not in connected:
                    self._departures[name] = Departure(name, tuple(worker.systems), now)
        # The hub's own record is more precise, and also covers workers
        # that came and went between two polls (or before a restart).
        for departed in snapshot.departed:
            if departed.name not in connected:
                self._departures[departed.name] = Departure(
                    departed.name, tuple(departed.systems), departed.departed_at
                )
        for name in connected:
            self._departures.pop(name, None)
        self._prune(now)
        self.snapshot = snapshot
        self.fetched_at = now
        self.error = None
        self.error_at = None

    def _prune(self, now: float) -> None:
        horizon = now - self.departure_window
        self._departures = {
            name: d for name, d in self._departures.items() if d.departed_at > horizon
        }

    def kind(self, name: str) -> WorkerKind:
        return "static" if name in self.expected_workers else "burst"

    def roster(self) -> list[WorkerView]:
        """Every connected, expected, and recently departed worker:
        static first in configured order, then burst by name."""
        if self.snapshot is None:
            return [
                WorkerView(name, "static", "unknown") for name in self.expected_workers
            ]
        # Departures age out on display even while the hub is
        # unreachable, so the page does not freeze on stale entries.
        self._prune(self._clock())
        connected = self.snapshot.sessions_by_name()
        views: list[WorkerView] = []
        for name in self.expected_workers:
            session = connected.get(name)
            if session is not None:
                views.append(WorkerView(name, "static", "up", session=session))
            else:
                departure = self._departures.get(name)
                views.append(
                    WorkerView(
                        name,
                        "static",
                        "down",
                        departed_at=departure.departed_at if departure else None,
                    )
                )
        burst = {
            name: WorkerView(name, "burst", "up", session=session)
            for name, session in connected.items()
            if name not in self.expected_workers
        }
        for name, departure in self._departures.items():
            if name not in self.expected_workers and name not in burst:
                burst[name] = WorkerView(
                    name, "burst", "departed", departed_at=departure.departed_at
                )
        views.extend(burst[name] for name in sorted(burst))
        return views

    async def worker_builds(
        self,
        registry: LogRegistry,
        queries: WebQueries,
        *,
        visible: Collection[int] | None,
        admin: bool,
    ) -> dict[str, WorkerBuilds]:
        """The last snapshot's assignments joined to running Nixbot
        derivations, filtered for the viewer (see join_builds)."""
        if self.snapshot is None or not self.snapshot.assignments:
            return {}
        running = registry.running_derivations()
        self.drv_outputs.retain(d.drv_path for d in running)
        # Unfiltered: join_builds applies visibility and counts the rest.
        builds = {b["id"]: b for b in await queries.queue()} if running else {}
        return join_builds(
            self.snapshot,
            running,
            self.drv_outputs,
            builds,
            visible=visible,
            admin=admin,
        )

    async def _run(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception:
                logger.exception("hub status poll crashed")
            await asyncio.sleep(self.interval)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._owns_http and self._http is not None:
            await self._http.aclose()
            self._http = None


def _describe(error: Exception) -> str:
    if isinstance(error, httpx.HTTPStatusError):
        return f"HTTP {error.response.status_code}"
    if isinstance(error, ValidationError):
        return "invalid status response"
    return str(error) or type(error).__name__


# --- joining assignments to running Nixbot derivations -------------------

# Leading outputs list of an ATerm .drv:
# Derive([("out","/nix/store/…-foo","",""),…],…
_DRV_OUTPUT = re.compile(r'\("([^"]*)","([^"]*)","[^"]*","[^"]*"\)')


def parse_drv_outputs(text: str) -> dict[str, str]:
    """Output name -> path of a .drv. Outputs without a known path
    (content-addressed) are left out."""
    if not text.startswith("Derive(["):
        msg = "not an ATerm derivation"
        raise ValueError(msg)
    outputs = text[len("Derive([") : text.index("],")]
    return {name: path for name, path in _DRV_OUTPUT.findall(outputs) if path}


class DrvOutputs:
    """Output paths per running drv, read once from the local .drv.

    For input-addressed derivations these are the scratch paths Nix
    hands Tribuchet, so they match the hub's assignment outputs.
    """

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, str]] = {}

    def get(self, drv_path: str) -> dict[str, str]:
        outputs = self._cache.get(drv_path)
        if outputs is None:
            try:
                outputs = parse_drv_outputs(Path(drv_path).read_text())
            except (OSError, UnicodeDecodeError, ValueError):
                logger.warning("cannot read derivation", extra={"drv": drv_path})
                outputs = {}
            self._cache[drv_path] = outputs
        return outputs

    def retain(self, drv_paths: Iterable[str]) -> None:
        """Forget derivations that are no longer running."""
        keep = set(drv_paths)
        self._cache = {d: o for d, o in self._cache.items() if d in keep}


@dataclass(frozen=True)
class BuildRow:
    """One assignment on a worker. Build fields are None for an
    assignment shown to an admin by name only."""

    worker: str
    name: str
    started_at: int  # hub assignment start, Unix seconds
    build_id: int | None = None
    attr: str | None = None
    drv_path: str | None = None
    forge: str | None = None
    owner: str | None = None
    project_name: str | None = None
    number: int | None = None


@dataclass
class WorkerBuilds:
    rows: list[BuildRow] = field(default_factory=list)
    # Assignments the viewer may not see, or that match no Nixbot build.
    hidden: int = 0


def join_builds(  # noqa: PLR0913
    snapshot: HubSnapshot,
    running: Iterable[RunningDrv],
    outputs: DrvOutputs,
    builds: Mapping[int, Mapping[str, Any]],
    *,
    visible: Collection[int] | None,
    admin: bool,
) -> dict[str, WorkerBuilds]:
    """Per-worker build rows for the viewer.

    An assignment matches a running drv on any output path. Admins also
    get a match on the derivation name, then a name-only row; other
    viewers count unmatched assignments, and builds of projects outside
    `visible` (None = all), as hidden.
    """
    by_output: dict[str, RunningDrv] = {}
    by_name: dict[str, list[RunningDrv]] = {}
    for drv in running:
        for path in outputs.get(drv.drv_path).values():
            by_output[path] = drv
        by_name.setdefault(drv.name, []).append(drv)

    result: dict[str, WorkerBuilds] = {}
    for assignment in snapshot.assignments:
        per_worker = result.setdefault(assignment.worker, WorkerBuilds())
        match: RunningDrv | None = next(
            (by_output[p] for p in assignment.outputs.values() if p in by_output),
            None,
        )
        if match is None and admin:
            named = by_name.get(assignment.name, [])
            match = named[0] if len(named) == 1 else None
        build = builds.get(match.build_id) if match is not None else None
        if match is None or build is None:
            if admin:
                per_worker.rows.append(
                    BuildRow(assignment.worker, assignment.name, assignment.started_at)
                )
            else:
                per_worker.hidden += 1
            continue
        if not admin and visible is not None and build["project_id"] not in visible:
            per_worker.hidden += 1
            continue
        per_worker.rows.append(
            BuildRow(
                worker=assignment.worker,
                name=match.name,
                started_at=assignment.started_at,
                build_id=match.build_id,
                attr=match.attr,
                drv_path=match.drv_path,
                forge=build["forge"],
                owner=build["owner"],
                project_name=build["project_name"],
                number=build["number"],
            )
        )
    return result
