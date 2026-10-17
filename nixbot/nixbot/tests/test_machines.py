"""Hub status poller: config, departure memory, and classification."""

# ruff: noqa: PLR2004 (literal values in test assertions are fine)

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from pydantic import ValidationError

from nixbot.bootstrap import build_service
from nixbot.config import Config, MachinesConfig
from nixbot.machines import DEPARTURE_WINDOW, MachinesMonitor, WorkerView

from .support import make_config

if TYPE_CHECKING:
    from pathlib import Path

STATUS_URL = "http://127.0.0.1:7438/status"
NOW = 1_792_215_429


def worker(name: str, *, connected_at: int = NOW - 3600, **kw: Any) -> dict[str, Any]:
    return {
        "name": name,
        "systems": ["x86_64-linux"],
        "max_jobs": 4,
        "free_slots": 4,
        "connected_at": connected_at,
        "last_heartbeat_at": NOW,
        **kw,
    }


def snapshot(*workers: dict[str, Any], **kw: Any) -> dict[str, Any]:
    return {
        "generated_at": NOW,
        "queue_depth": 0,
        "workers": list(workers),
        "assignments": [],
        "queued": [],
        "departed": [],
        **kw,
    }


class Clock:
    def __init__(self) -> None:
        self.now = float(NOW)

    def __call__(self) -> float:
        return self.now


class Hub:
    """Serves queued responses: a dict is a snapshot, an exception is
    raised as a transport failure, an int is an HTTP status."""

    def __init__(self) -> None:
        self.responses: list[Any] = []
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if isinstance(response, int):
            return httpx.Response(response)
        return httpx.Response(200, json=response)


def monitor(
    hub: Hub, clock: Clock, expected: list[str] | None = None
) -> MachinesMonitor:
    return MachinesMonitor(
        MachinesConfig(status_url=STATUS_URL, expected_workers=expected or []),
        http=httpx.AsyncClient(transport=httpx.MockTransport(hub.handler)),
        clock=clock,
    )


def states(views: list[WorkerView]) -> list[tuple[str, str, str]]:
    return [(v.name, v.kind, v.state) for v in views]


# --- config --------------------------------------------------------------


def test_machines_config_defaults_off() -> None:
    config = Config(build_systems=["x86_64-linux"], url="http://ci.test")
    assert config.machines is None


def test_machines_config_parses() -> None:
    config = Config.model_validate(
        {
            "build_systems": ["x86_64-linux"],
            "url": "http://ci.test",
            "machines": {
                "status_url": STATUS_URL,
                "expected_workers": ["netc", "local-san-dev-framework-1"],
            },
        }
    )
    assert config.machines is not None
    assert config.machines.status_url == STATUS_URL
    assert config.machines.expected_workers == ["netc", "local-san-dev-framework-1"]


@pytest.mark.parametrize(
    "url", ["127.0.0.1:7438/status", "/status", "unix:///run/hub.sock", "http://"]
)
def test_machines_config_rejects_non_url(url: str) -> None:
    with pytest.raises(ValidationError, match="status_url"):
        Config.model_validate(
            {
                "build_systems": ["x86_64-linux"],
                "url": "http://ci.test",
                "machines": {"status_url": url},
            }
        )


def test_machines_config_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        MachinesConfig.model_validate({"status_url": STATUS_URL, "expected": []})


# --- bootstrap -----------------------------------------------------------


async def test_unconfigured_leaves_machines_unset(
    postgres_dsn: str, tmp_path: Path
) -> None:
    service, app = await build_service(make_config(postgres_dsn, tmp_path / "state"))
    try:
        assert app.state.web_context.machines is None
    finally:
        await service.pool.close()


async def test_configured_wires_idle_monitor(postgres_dsn: str, tmp_path: Path) -> None:
    config = make_config(
        postgres_dsn,
        tmp_path / "state",
        machines=MachinesConfig(status_url=STATUS_URL, expected_workers=["netc"]),
    )
    service, app = await build_service(config)
    try:
        machines = app.state.web_context.machines
        assert isinstance(machines, MachinesMonitor)
        assert machines.expected_workers == ("netc",)
        # run_service starts it; building the service does not.
        assert not machines.running
    finally:
        await service.pool.close()


# --- polling and classification -----------------------------------------


async def test_poll_fetches_status_url() -> None:
    hub, clock = Hub(), Clock()
    hub.responses.append(snapshot(worker("netc"), queue_depth=2))
    machines = monitor(hub, clock)
    assert await machines.poll_once()
    assert str(hub.requests[0].url) == STATUS_URL
    assert machines.snapshot is not None
    assert machines.snapshot.queue_depth == 2
    assert machines.fetched_at == clock.now
    assert machines.reachable


async def test_parses_documented_snapshot() -> None:
    """The example from Tribuchet's README parses in full."""
    hub, clock = Hub(), Clock()
    hub.responses.append(
        {
            "generated_at": NOW,
            "queue_depth": 1,
            "workers": [
                worker("builder-a", free_slots=3, connected_at=1700000001),
                worker(
                    "builder-b",
                    systems=["aarch64-linux"],
                    free_slots=0,
                    connected_at=1700000002,
                    last_heartbeat_at=None,
                ),
            ],
            "assignments": [
                {
                    "job_id": "aaaa",
                    "worker": "builder-a",
                    "system": "x86_64-linux",
                    "name": "hello-2.12",
                    "outputs": {"out": "/nix/store/aaaa-hello-2.12"},
                    "started_at": NOW,
                }
            ],
            "queued": [
                {
                    "job_id": "bbbb",
                    "system": "x86_64-linux",
                    "name": "world-1.0",
                    "outputs": {"out": "/nix/store/bbbb-world-1.0"},
                    "enqueued_at": 1700000000,
                }
            ],
            "departed": [
                {"name": "builder-c", "systems": ["x86_64-linux"], "departed_at": NOW}
            ],
            "future_field": True,
        }
    )
    machines = monitor(hub, clock)
    assert await machines.poll_once()
    snap = machines.snapshot
    assert snap is not None
    assert snap.workers[1].last_heartbeat_at is None
    assert snap.assignments[0].outputs == {"out": "/nix/store/aaaa-hello-2.12"}
    assert snap.queued[0].name == "world-1.0"
    assert states(machines.roster()) == [
        ("builder-a", "burst", "up"),
        ("builder-b", "burst", "up"),
        ("builder-c", "burst", "departed"),
    ]


async def test_expected_worker_present_is_static_and_up() -> None:
    hub, clock = Hub(), Clock()
    hub.responses.append(snapshot(worker("netc")))
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    [view] = machines.roster()
    assert (view.name, view.kind, view.state) == ("netc", "static", "up")
    assert view.session is not None
    assert view.session.max_jobs == 4


async def test_expected_worker_absent_is_static_and_down() -> None:
    """Covers AE2."""
    hub, clock = Hub(), Clock()
    hub.responses.append(snapshot(worker("netc")))
    machines = monitor(hub, clock, expected=["netc", "local-san-dev-framework-1"])
    await machines.poll_once()
    assert states(machines.roster()) == [
        ("netc", "static", "up"),
        ("local-san-dev-framework-1", "static", "down"),
    ]


async def test_unlisted_worker_is_burst() -> None:
    hub, clock = Hub(), Clock()
    hub.responses.append(snapshot(worker("netc"), worker("i-0abc")))
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    assert states(machines.roster()) == [
        ("netc", "static", "up"),
        ("i-0abc", "burst", "up"),
    ]


async def test_static_first_then_burst_by_name() -> None:
    hub, clock = Hub(), Clock()
    hub.responses.append(snapshot(worker("i-b"), worker("i-a"), worker("zeta")))
    machines = monitor(hub, clock, expected=["zeta", "alpha"])
    await machines.poll_once()
    assert [v.name for v in machines.roster()] == ["zeta", "alpha", "i-a", "i-b"]


async def test_departed_burst_worker_expires_after_window() -> None:
    """Covers AE3: a burst worker present in one poll and absent in the
    next is recently departed, then gone after the window."""
    hub, clock = Hub(), Clock()
    hub.responses += [
        snapshot(worker("netc"), worker("i-0abc")),
        snapshot(worker("netc")),
        snapshot(worker("netc")),
    ]
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    clock.now += 5
    await machines.poll_once()
    departed_at = clock.now
    views = machines.roster()
    assert states(views) == [
        ("netc", "static", "up"),
        ("i-0abc", "burst", "departed"),
    ]
    assert views[1].departed_at == departed_at

    clock.now = departed_at + DEPARTURE_WINDOW - 1
    assert states(machines.roster())[1] == ("i-0abc", "burst", "departed")

    clock.now = departed_at + DEPARTURE_WINDOW + 1
    await machines.poll_once()
    assert states(machines.roster()) == [("netc", "static", "up")]


async def test_hub_departed_list_is_remembered_past_its_grace() -> None:
    """A worker that came and went between two polls is known only from
    the hub's departed list, which forgets it after ~30s."""
    hub, clock = Hub(), Clock()
    hub.responses += [
        snapshot(
            departed=[
                {"name": "i-0abc", "systems": ["x86_64-linux"], "departed_at": NOW - 4}
            ]
        ),
        snapshot(),
    ]
    machines = monitor(hub, clock)
    await machines.poll_once()
    clock.now += 60
    await machines.poll_once()
    [view] = machines.roster()
    assert (view.name, view.state, view.departed_at) == ("i-0abc", "departed", NOW - 4)


async def test_reconnect_clears_departure() -> None:
    hub, clock = Hub(), Clock()
    hub.responses += [
        snapshot(worker("i-0abc")),
        snapshot(),
        snapshot(worker("i-0abc", connected_at=NOW)),
    ]
    machines = monitor(hub, clock)
    for _ in range(3):
        await machines.poll_once()
        clock.now += 5
    assert states(machines.roster()) == [("i-0abc", "burst", "up")]


async def test_down_static_worker_keeps_departure_time() -> None:
    hub, clock = Hub(), Clock()
    hub.responses += [snapshot(worker("netc")), snapshot()]
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    clock.now += 5
    await machines.poll_once()
    [view] = machines.roster()
    assert (view.kind, view.state, view.departed_at) == ("static", "down", clock.now)


async def test_overlapping_reconnect_lists_worker_once() -> None:
    """During an overlapping reconnect the hub lists one name twice and
    may also report the old session as departed. The newest session
    wins and the worker is up."""
    hub, clock = Hub(), Clock()
    hub.responses.append(
        snapshot(
            worker("netc", connected_at=NOW, free_slots=4),
            worker("netc", connected_at=NOW - 3600, free_slots=1),
            departed=[{"name": "netc", "systems": [], "departed_at": NOW}],
        )
    )
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    [view] = machines.roster()
    assert view.state == "up"
    assert view.session is not None
    assert view.session.connected_at == NOW


@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("refused"), 503, {"not": "a snapshot"}],
    ids=["connect", "http-503", "invalid-body"],
)
async def test_failed_poll_keeps_last_snapshot(failure: Any) -> None:
    """Covers AE1: a failing poll keeps the last snapshot, records the
    error and its time, and does not mark workers down."""
    hub, clock = Hub(), Clock()
    hub.responses += [snapshot(worker("netc"), worker("i-0abc")), failure]
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    fetched_at = clock.now
    clock.now += 5
    assert not await machines.poll_once()
    assert machines.error
    assert machines.error_at == clock.now
    assert machines.fetched_at == fetched_at
    assert not machines.reachable
    assert states(machines.roster()) == [
        ("netc", "static", "up"),
        ("i-0abc", "burst", "up"),
    ]


async def test_recovery_clears_error() -> None:
    hub, clock = Hub(), Clock()
    hub.responses += [httpx.ConnectError("refused"), snapshot(worker("netc"))]
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    await machines.poll_once()
    assert machines.error is None
    assert machines.error_at is None
    assert machines.reachable


async def test_unreachable_before_first_snapshot_is_unknown() -> None:
    """With no snapshot yet, expected workers are unknown, not down."""
    hub, clock = Hub(), Clock()
    hub.responses.append(httpx.ConnectError("refused"))
    machines = monitor(hub, clock, expected=["netc"])
    await machines.poll_once()
    assert machines.snapshot is None
    assert machines.error == "refused"
    assert states(machines.roster()) == [("netc", "static", "unknown")]


async def test_start_polls_until_stopped() -> None:
    hub, clock = Hub(), Clock()
    hub.responses += [snapshot(worker("netc"))] * 50
    machines = monitor(hub, clock)
    machines.interval = 0
    machines.start()
    assert machines.running
    for _ in range(100):
        if len(hub.requests) >= 2:
            break
        await asyncio.sleep(0)
    await machines.stop()
    assert not machines.running
    assert len(hub.requests) >= 2
    assert machines.reachable
