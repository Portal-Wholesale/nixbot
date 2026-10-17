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
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from .config import MachinesConfig

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
