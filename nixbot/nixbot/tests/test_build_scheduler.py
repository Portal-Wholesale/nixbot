"""Tests for the service job scheduler, enumerating the behavioral
branches of nixbot/build_trigger.py."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from nixbot.build_scheduler import (
    AttributeResult,
    AttributeStatus,
    BuildOutcome,
    CachedFailure,
    FailFastStop,
    JobScheduler,
    ScheduleResult,
    _success_result,
    compute_job_closures,
    get_failed_dependents,
    outcome_status,
    sort_jobs_by_closures,
)
from nixbot.db import FAILED_ATTRIBUTE_STATUSES, TERMINAL_ATTRIBUTE_STATUSES
from nixbot.models import (
    CacheStatus,
    NixEvalJob,
    NixEvalJobError,
    NixEvalJobSuccess,
)

from .support import FakeCache, mk_job

SYSTEM = "x86_64-linux"


class FakeExecutor:
    """Records build order. Per-attr outcomes configurable."""

    def __init__(self, outcomes: dict[str, BuildOutcome] | None = None) -> None:
        self.outcomes = outcomes or {}
        self.built: list[str] = []

    async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
        self.built.append(job.attr)
        await asyncio.sleep(0)
        return self.outcomes.get(job.attr, BuildOutcome.success)


async def run_scheduler(scheduler: JobScheduler, jobs: list) -> ScheduleResult:
    return await scheduler.run(jobs)


def by_attr(result: ScheduleResult) -> dict[str, AttributeStatus]:
    return {r.attr: r.status for r in result.results}


async def test_topological_order() -> None:
    # c depends on b depends on a.
    a, b, c = mk_job("a"), mk_job("b", deps=["a"]), mk_job("c", deps=["b"])
    closures = compute_job_closures([c, a, b])
    assert closures[c.drv_path] == {b.drv_path}
    assert closures[b.drv_path] == {a.drv_path}
    order = sort_jobs_by_closures([c, a, b], closures)
    assert [j.attr for j in order] == ["a", "b", "c"]

    # A running build (b) is not keyed but keeps c's edge to it alive.
    running = compute_job_closures([c], frozenset({b.drv_path}))
    assert running == {c.drv_path: {b.drv_path}}

    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [c, a, b])
    assert executor.built.index("a") < executor.built.index("b")
    assert executor.built.index("b") < executor.built.index("c")
    assert result.success


def test_get_failed_dependents_transitive() -> None:
    a, b, c, d = (
        mk_job("a"),
        mk_job("b", deps=["a"]),
        mk_job("c", deps=["b"]),
        mk_job("d"),
    )
    closures = compute_job_closures([a, b, c, d])
    removed = get_failed_dependents(a, [b, c, d], closures)
    assert {j.attr for j in removed} == {"b", "c"}


async def test_dependency_failure_propagation() -> None:
    a, b, c, d = (
        mk_job("a"),
        mk_job("b", deps=["a"]),
        mk_job("c", deps=["b"]),
        mk_job("d"),
    )
    executor = FakeExecutor({"a": BuildOutcome.failure})
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [a, b, c, d])
    statuses = by_attr(result)
    assert statuses["a"] == AttributeStatus.failed
    assert statuses["b"] == AttributeStatus.dependency_failed
    assert statuses["c"] == AttributeStatus.dependency_failed
    assert statuses["d"] == AttributeStatus.succeeded
    assert "b" not in executor.built
    assert "c" not in executor.built
    deps = {r.attr: r.dependency_attr for r in result.results}
    assert deps["b"] == "a"
    assert deps["c"] == "a"
    assert not result.success


async def test_supported_systems_filter() -> None:
    native = mk_job("native")
    foreign = mk_job("foreign", system="riscv64-linux")
    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [native, foreign])
    assert executor.built == ["native"]
    assert "foreign" not in by_attr(result)


async def test_local_jobs_skipped_with_out_path_recorded() -> None:
    local = mk_job("local", cache_status=CacheStatus.local)
    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [local])
    assert executor.built == []
    assert by_attr(result)["local"] == AttributeStatus.skipped_local
    assert result.skipped_out_paths == [("local", "/nix/store/local-out")]
    assert result.success


async def test_local_job_unblocks_dependents() -> None:
    base = mk_job("base", cache_status=CacheStatus.local)
    top = mk_job("top", deps=["base"])
    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [base, top])
    assert executor.built == ["top"]
    assert result.success


async def test_cached_jobs_scheduled_for_substitution() -> None:
    cached = mk_job("cached", cache_status=CacheStatus.cached)
    executor = FakeExecutor()
    await run_scheduler(JobScheduler(executor, [SYSTEM]), [cached])
    assert executor.built == ["cached"]


async def test_failed_eval_records() -> None:
    bad = NixEvalJobError(error="boom", attr="bad", attr_path=["bad"])
    good = mk_job("good")
    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [bad, good])
    statuses = by_attr(result)
    assert statuses["bad"] == AttributeStatus.failed_eval
    assert statuses["good"] == AttributeStatus.succeeded
    errors = {r.attr: r.error for r in result.results}
    assert errors["bad"] == "boom"
    assert not result.success


async def test_cached_failure_skip() -> None:
    job = mk_job("flaky")
    cache = FakeCache(
        {
            job.drv_path: CachedFailure(
                drv_path=job.drv_path,
                time=datetime(2026, 1, 1, tzinfo=UTC),
                url="http://ci/builds/1",
            )
        }
    )
    executor = FakeExecutor()
    result = await run_scheduler(
        JobScheduler(executor, [SYSTEM], failed_build_cache=cache), [job]
    )
    assert executor.built == []
    record = result.results[0]
    assert record.status == AttributeStatus.cached_failure
    assert record.first_failure_url == "http://ci/builds/1"
    assert not result.success


async def test_cached_failure_propagates_to_dependents() -> None:
    base = mk_job("base")
    top = mk_job("top", deps=["base"])
    cache = FakeCache(
        {
            base.drv_path: CachedFailure(
                drv_path=base.drv_path, time=datetime(2026, 1, 1, tzinfo=UTC), url="u"
            )
        }
    )
    executor = FakeExecutor()
    result = await run_scheduler(
        JobScheduler(executor, [SYSTEM], failed_build_cache=cache), [base, top]
    )
    statuses = by_attr(result)
    assert statuses["base"] == AttributeStatus.cached_failure
    assert statuses["top"] == AttributeStatus.dependency_failed
    assert executor.built == []


async def test_failure_recorded_in_cache() -> None:
    job = mk_job("breaks")
    cache = FakeCache()
    executor = FakeExecutor({"breaks": BuildOutcome.failure})
    await run_scheduler(
        JobScheduler(
            executor, [SYSTEM], failed_build_cache=cache, build_url="http://ci/b/2"
        ),
        [job],
    )
    assert cache.added == [(job.drv_path, "http://ci/b/2")]


async def test_cancelled_not_recorded_in_cache_and_propagates_cancelled() -> None:
    a = mk_job("a")
    b = mk_job("b", deps=["a"])
    cache = FakeCache()
    executor = FakeExecutor({"a": BuildOutcome.cancelled})
    result = await run_scheduler(
        JobScheduler(executor, [SYSTEM], failed_build_cache=cache), [a, b]
    )
    assert cache.added == []
    statuses = by_attr(result)
    assert statuses["a"] == AttributeStatus.cancelled
    # Cancelled propagates "cancelled/skipped", never dependency_failed.
    assert statuses["b"] == AttributeStatus.cancelled


async def test_mixed_eval_results_independent() -> None:
    bad = NixEvalJobError(error="nope", attr="bad", attr_path=["bad"])
    a = mk_job("a")
    b = mk_job("b", deps=["a"])
    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [bad, a, b])
    statuses = by_attr(result)
    assert statuses == {
        "bad": AttributeStatus.failed_eval,
        "a": AttributeStatus.succeeded,
        "b": AttributeStatus.succeeded,
    }
    assert not result.success


async def test_skipped_local_frees_dependents_immediately() -> None:
    # parent is already in the local store (skipped). Its dependent must
    # be dispatched right away, not only after the unrelated running
    # build finishes.
    parent = mk_job("parent", cache_status=CacheStatus.local)
    child = mk_job("child", deps=["parent"])
    other = mk_job("other")

    started_child = asyncio.Event()

    class BlockingExecutor(FakeExecutor):
        async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
            self.built.append(job.attr)
            if job.attr == "other":
                # Holds a slot until the child has been dispatched.
                await asyncio.wait_for(started_child.wait(), timeout=5)
            elif job.attr == "child":
                started_child.set()
            return BuildOutcome.success

    executor = BlockingExecutor()
    result = await asyncio.wait_for(
        JobScheduler(executor, [SYSTEM]).run([other, parent, child]), timeout=5
    )
    statuses = by_attr(result)
    assert statuses["parent"] == AttributeStatus.skipped_local
    assert statuses["child"] == AttributeStatus.succeeded
    assert statuses["other"] == AttributeStatus.succeeded


async def test_post_build_failure_not_cached_and_dependents_build() -> None:
    # A post-build-step failure fails the attribute but must not poison
    # the failed-build cache (the derivation built fine) nor fail
    # dependents.
    parent = mk_job("parent")
    child = mk_job("child", deps=["parent"])
    cache = FakeCache()
    executor = FakeExecutor({"parent": BuildOutcome.post_build_failure})
    result = await run_scheduler(
        JobScheduler(executor, [SYSTEM], failed_build_cache=cache), [parent, child]
    )
    statuses = by_attr(result)
    assert statuses["parent"] == AttributeStatus.failed
    assert statuses["child"] == AttributeStatus.succeeded
    assert "child" in executor.built
    assert cache.added == []


async def test_cached_failure_error_includes_url() -> None:
    job = mk_job("a")
    cached = CachedFailure(
        drv_path=job.drv_path,
        time=datetime(2026, 1, 1, tzinfo=UTC),
        url="https://ci.example/builds/1",
    )
    cache = FakeCache({job.drv_path: cached})
    result = await run_scheduler(
        JobScheduler(FakeExecutor(), [SYSTEM], failed_build_cache=cache), [job]
    )
    (res,) = result.results
    assert res.status == AttributeStatus.cached_failure
    assert res.error is not None
    assert "https://ci.example/builds/1" in res.error


async def test_on_result_reports_skips_before_builds_finish() -> None:
    """on_result fires for skips while other jobs are still running."""
    reported: list[tuple[str, str]] = []
    skip_seen = asyncio.Event()

    async def on_result(result: AttributeResult) -> None:
        reported.append((result.attr, result.status.value))
        if result.status == AttributeStatus.skipped_local:
            skip_seen.set()

    class BlockingExecutor(FakeExecutor):
        async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
            # Deadlocks unless the skip was reported before builds finish.
            await asyncio.wait_for(skip_seen.wait(), timeout=5)
            return await super().build(job)

    scheduler = JobScheduler(BlockingExecutor(), [SYSTEM], on_result=on_result)
    jobs: list[NixEvalJob] = [
        mk_job("local", cache_status=CacheStatus.local),
        mk_job("fresh", cache_status=CacheStatus.not_built),
    ]
    result = await scheduler.run(jobs)
    assert ("local", "skipped_local") in reported
    assert ("fresh", "succeeded") in reported
    assert {r.attr for r in result.results} == {"local", "fresh"}


async def test_streaming_builds_start_before_eval_finishes() -> None:
    # Jobs from early batches must be dispatched while later batches
    # (and the end-of-eval sentinel) are still outstanding.
    first_built = asyncio.Event()

    class SignallingExecutor(FakeExecutor):
        async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
            first_built.set()
            return await super().build(job)

    async def main() -> None:
        executor = SignallingExecutor()
        scheduler = JobScheduler(executor, [SYSTEM])
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        await queue.put([mk_job("early")])
        run_task = asyncio.create_task(scheduler.run_incremental(queue))
        # The first job must build without the sentinel being queued.
        await asyncio.wait_for(first_built.wait(), timeout=5)
        await queue.put([mk_job("late")])
        await queue.put(None)
        result = await asyncio.wait_for(run_task, timeout=5)
        assert {r.attr for r in result.results} == {"early", "late"}
        assert all(r.status == AttributeStatus.succeeded for r in result.results)

    await main()


async def test_streaming_dependency_across_batches() -> None:
    # A job arriving in a later batch can depend on one from an
    # earlier batch that already finished. It must still build.
    async def main() -> None:
        executor = FakeExecutor()
        scheduler = JobScheduler(executor, [SYSTEM])
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        await queue.put([mk_job("dep")])
        await queue.put([mk_job("top", deps=["dep"])])
        await queue.put(None)
        result = await asyncio.wait_for(
            asyncio.create_task(scheduler.run_incremental(queue)), timeout=5
        )
        assert {r.attr for r in result.results} == {"dep", "top"}
        assert all(r.status == AttributeStatus.succeeded for r in result.results)

    await main()


async def test_streaming_failed_dep_fails_late_dependent() -> None:
    # A later-batch job depending on an already-failed job must be
    # marked dependency_failed, not built.
    dep_failed = asyncio.Event()

    async def on_result(result: AttributeResult) -> None:
        if result.attr == "dep":
            dep_failed.set()

    async def main() -> None:
        executor = FakeExecutor({"dep": BuildOutcome.failure})
        scheduler = JobScheduler(executor, [SYSTEM], on_result=on_result)
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        await queue.put([mk_job("dep")])
        run_task = asyncio.create_task(scheduler.run_incremental(queue))
        # Let the failure settle before the dependent arrives.
        await asyncio.wait_for(dep_failed.wait(), timeout=5)
        await queue.put([mk_job("top", deps=["dep"])])
        await queue.put(None)
        result = await asyncio.wait_for(run_task, timeout=5)
        statuses = {r.attr: r.status for r in result.results}
        assert statuses["dep"] == AttributeStatus.failed
        assert statuses["top"] == AttributeStatus.dependency_failed
        assert "top" not in executor.built

    await main()


async def test_streaming_cancelled_dep_cancels_late_dependent() -> None:
    # A later-batch job depending on an already-cancelled (superseded)
    # job must settle as cancelled, not dependency_failed: batch timing
    # must not flip a superseded build from CANCELLED to FAILED.
    dep_cancelled = asyncio.Event()

    async def on_result(result: AttributeResult) -> None:
        if result.attr == "dep":
            dep_cancelled.set()

    async def main() -> None:
        executor = FakeExecutor({"dep": BuildOutcome.cancelled})
        scheduler = JobScheduler(executor, [SYSTEM], on_result=on_result)
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        await queue.put([mk_job("dep")])
        run_task = asyncio.create_task(scheduler.run_incremental(queue))
        # Let the cancellation settle before the dependent arrives.
        await asyncio.wait_for(dep_cancelled.wait(), timeout=5)
        await queue.put([mk_job("top", deps=["dep"])])
        await queue.put(None)
        result = await asyncio.wait_for(run_task, timeout=5)
        statuses = by_attr(result)
        assert statuses["dep"] == AttributeStatus.cancelled
        assert statuses["top"] == AttributeStatus.cancelled
        assert "top" not in executor.built

    await main()


async def test_streaming_batch_order_does_not_leak_failed_dependency() -> None:
    # Within one batch, a dependent listed before its dependency-failed
    # sibling must still settle as dependency_failed, not build.
    dep_failed = asyncio.Event()

    async def on_result(result: AttributeResult) -> None:
        if result.attr == "f":
            dep_failed.set()

    async def main() -> None:
        executor = FakeExecutor({"f": BuildOutcome.failure})
        scheduler = JobScheduler(executor, [SYSTEM], on_result=on_result)
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        await queue.put([mk_job("f")])
        run_task = asyncio.create_task(scheduler.run_incremental(queue))
        await asyncio.wait_for(dep_failed.wait(), timeout=5)
        # "top" precedes "mid" in the batch. Only "mid" depends on the
        # failed job directly.
        await queue.put([mk_job("top", deps=["mid"]), mk_job("mid", deps=["f"])])
        await queue.put(None)
        result = await asyncio.wait_for(run_task, timeout=5)
        statuses = {r.attr: r.status for r in result.results}
        assert statuses["mid"] == AttributeStatus.dependency_failed
        assert statuses["top"] == AttributeStatus.dependency_failed
        assert executor.built == ["f"]

    await main()


async def test_streaming_pending_dependent_fails_when_late_dep_arrives_failed() -> None:
    # A pending job whose dependency arrives in a later batch already
    # dependency-failed must settle as dependency_failed, not dispatch.
    f_failed = asyncio.Event()
    a_settled = asyncio.Event()
    release_r = asyncio.Event()
    r_building = asyncio.Event()

    async def on_result(result: AttributeResult) -> None:
        if result.attr == "f":
            f_failed.set()
        if result.attr == "a":
            a_settled.set()

    class GatedExecutor(FakeExecutor):
        async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
            if job.attr == "r":
                r_building.set()
                await asyncio.wait_for(release_r.wait(), timeout=5)
            return await super().build(job)

    async def main() -> None:
        executor = GatedExecutor({"f": BuildOutcome.failure})
        scheduler = JobScheduler(executor, [SYSTEM], on_result=on_result)
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        await queue.put([mk_job("f"), mk_job("r")])
        run_task = asyncio.create_task(scheduler.run_incremental(queue))
        await asyncio.wait_for(f_failed.wait(), timeout=5)
        await asyncio.wait_for(r_building.wait(), timeout=5)
        # "top" is kept pending by the running "r" while its other
        # dependency "a" arrives later, already dependency-failed.
        await queue.put([mk_job("top", deps=["r", "a"])])
        await queue.put([mk_job("a", deps=["f"])])
        await queue.put(None)
        # "top" must settle while "r" still runs. Only then release.
        await asyncio.wait_for(a_settled.wait(), timeout=5)
        release_r.set()
        result = await asyncio.wait_for(run_task, timeout=5)
        statuses = {r.attr: r.status for r in result.results}
        assert statuses["a"] == AttributeStatus.dependency_failed
        assert statuses["top"] == AttributeStatus.dependency_failed
        assert "top" not in executor.built

    await main()


async def test_local_store_skip_wins_over_failure_cache() -> None:
    # An attribute whose output already exists in the local store is
    # not a failure, even when the failed-build cache still carries a
    # stale entry for its drv: skip it, do not fail its dependents.
    a = mk_job("a", cache_status=CacheStatus.local)
    b = mk_job("b", deps=["a"])
    cache = FakeCache(
        {
            a.drv_path: CachedFailure(
                drv_path=a.drv_path,
                time=datetime(2024, 1, 1, tzinfo=UTC),
                url="http://x",
            )
        }
    )
    executor = FakeExecutor()
    result = await run_scheduler(
        JobScheduler(executor, [SYSTEM], failed_build_cache=cache), [a, b]
    )
    statuses = by_attr(result)
    assert statuses["a"] == AttributeStatus.skipped_local
    assert statuses["b"] == AttributeStatus.succeeded
    assert executor.built == ["b"]


async def test_missing_cache_status_means_build() -> None:
    # nix-eval-jobs output without cacheStatus must not be treated as
    # "already in the local store": a whole eval would report success
    # without building anything.
    job = NixEvalJobSuccess.model_validate(
        {
            "attr": "foo",
            "attrPath": ["foo"],
            "neededBuilds": ["/nix/store/foo.drv"],
            "neededSubstitutes": [],
            "drvPath": "/nix/store/foo.drv",
            "name": "foo",
            "outputs": {"out": "/nix/store/foo-out"},
            "system": SYSTEM,
        }
    )
    assert job.cache_status is None
    executor = FakeExecutor()
    result = await run_scheduler(JobScheduler(executor, [SYSTEM]), [job])
    assert executor.built == ["foo"]
    assert by_attr(result)["foo"] == AttributeStatus.succeeded


async def test_resent_jobs_are_not_rebuilt() -> None:
    """At-least-once delivery: a re-sent batch must not rebuild."""

    async def main() -> None:
        executor = FakeExecutor()
        scheduler = JobScheduler(executor, [SYSTEM])
        queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
        jobs = [mk_job("a"), mk_job("b", deps=["a"])]
        await queue.put([jobs[0]])
        await queue.put([jobs[1]])
        await queue.put(list(jobs))  # final re-send
        await queue.put(None)
        result = await asyncio.wait_for(scheduler.run_incremental(queue), timeout=5)
        assert sorted(executor.built) == ["a", "b"]
        assert sorted(r.attr for r in result.results) == ["a", "b"]

    await main()


def _with_extra(job: NixEvalJobSuccess, **flags: bool) -> NixEvalJobSuccess:
    return job.model_copy(update={"extra_value": flags})


def test_require_failure_only_accepts_genuine_build_failures() -> None:
    job = _with_extra(mk_job("neg"), requireFailure=True)
    assert outcome_status(job, BuildOutcome.failure) == (
        AttributeStatus.succeeded,
        None,
    )
    # Timeouts/transient errors and unexpected successes stay failures.
    assert outcome_status(job, BuildOutcome.failure_no_cache)[0] == (
        AttributeStatus.failed
    )
    assert outcome_status(job, BuildOutcome.success)[0] == AttributeStatus.failed


def test_ignore_failure_status_is_not_a_failure() -> None:
    job = _with_extra(mk_job("soft"), ignoreFailure=True)
    assert outcome_status(job, BuildOutcome.failure) == (
        AttributeStatus.ignored_failure,
        None,
    )
    assert "ignored_failure" not in FAILED_ATTRIBUTE_STATUSES
    assert "ignored_failure" in TERMINAL_ATTRIBUTE_STATUSES


def test_deps_only_result_has_no_out_path() -> None:
    job = _with_extra(mk_job("shell"), buildDependenciesOnly=True)
    result = _success_result(job, AttributeStatus.succeeded)
    assert result.out_path is None


# --- fail-fast -------------------------------------------------------------


def _cache_for(job: NixEvalJobSuccess) -> FakeCache:
    return FakeCache(
        {
            job.drv_path: CachedFailure(
                drv_path=job.drv_path,
                time=datetime(2026, 1, 1, tzinfo=UTC),
                url="http://ci/builds/1",
            )
        }
    )


async def _run_fail_fast(
    bad: NixEvalJobSuccess,
    outcome: BuildOutcome,
    *,
    cache: FakeCache | None = None,
    fail_fast: FailFastStop | None = None,
) -> tuple[FakeExecutor, ScheduleResult]:
    """`bad` settles with `outcome` while `slow` is still running and
    `x` waits for `slow`. `slow` finishes once `bad` is reported."""
    bad_reported = asyncio.Event()

    class GatedExecutor(FakeExecutor):
        async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
            if job.attr == "slow":
                await bad_reported.wait()
            return await super().build(job)

    async def on_result(result: AttributeResult) -> None:
        if result.attr == bad.attr:
            bad_reported.set()

    executor = GatedExecutor({bad.attr: outcome})
    scheduler = JobScheduler(
        executor,
        [SYSTEM],
        failed_build_cache=cache,
        on_result=on_result,
        fail_fast=fail_fast,
    )
    jobs: list[NixEvalJob] = [bad, mk_job("slow"), mk_job("x", deps=["slow"])]
    result = await asyncio.wait_for(scheduler.run(jobs), timeout=5)
    return executor, result


def test_fail_fast_stop_modes() -> None:
    drain = FailFastStop("drain")
    assert drain.fire("a")
    assert drain.stop_queued.is_set()
    assert not drain.stop_running.is_set()
    # Only the first call wins.
    assert not drain.fire("b")
    assert drain.trigger_attr == "a"
    assert drain.reason == "stopped: fail-fast after a"

    cancel = FailFastStop("cancel")
    assert cancel.fire("a")
    assert cancel.stop_queued.is_set()
    assert cancel.stop_running.is_set()


@pytest.mark.parametrize(
    ("bad", "outcome", "cached"),
    [
        (mk_job("bad"), BuildOutcome.failure, False),
        # The build succeeded although requireFailure is set.
        (_with_extra(mk_job("bad"), requireFailure=True), BuildOutcome.success, False),
        (mk_job("bad"), BuildOutcome.success, True),
    ],
    ids=["failed", "require-failure-succeeded", "cached-failure"],
)
async def test_fail_fast_triggering_status_stops(
    bad: NixEvalJobSuccess, outcome: BuildOutcome, *, cached: bool
) -> None:
    stop = FailFastStop("drain")
    executor, result = await _run_fail_fast(
        bad, outcome, cache=_cache_for(bad) if cached else None, fail_fast=stop
    )
    assert stop.trigger_attr == "bad"
    assert stop.stop_queued.is_set()
    statuses = by_attr(result)
    assert statuses["bad"] in {AttributeStatus.failed, AttributeStatus.cached_failure}
    # The pending job never builds; the running one drains.
    assert "x" not in executor.built
    assert statuses["x"] == AttributeStatus.cancelled
    if cached:
        # A cached failure fires while dispatching, before slow starts.
        assert "slow" not in executor.built
        assert statuses["slow"] == AttributeStatus.cancelled
    else:
        assert statuses["slow"] == AttributeStatus.succeeded
    errors = {r.attr: r.error for r in result.results}
    assert errors["x"] == "stopped: fail-fast after bad"
    assert not result.success


@pytest.mark.parametrize(
    ("bad", "outcome"),
    [
        (_with_extra(mk_job("bad"), ignoreFailure=True), BuildOutcome.failure),
        (mk_job("bad"), BuildOutcome.post_build_failure),
        # An internal error settles failed but is not a build failure.
        (mk_job("bad"), BuildOutcome.failure_no_cache),
        (_with_extra(mk_job("bad"), requireFailure=True), BuildOutcome.failure),
        (mk_job("bad"), BuildOutcome.cancelled),
    ],
    ids=[
        "ignored-failure",
        "post-build-failure",
        "internal-error",
        "require-failure-met",
        "cancelled",
    ],
)
async def test_fail_fast_non_triggering_status_does_not_stop(
    bad: NixEvalJobSuccess, outcome: BuildOutcome
) -> None:
    stop = FailFastStop("cancel")
    executor, result = await _run_fail_fast(bad, outcome, fail_fast=stop)
    assert not stop.fired
    assert not stop.stop_queued.is_set()
    assert "x" in executor.built
    assert by_attr(result)["x"] == AttributeStatus.succeeded


async def test_fail_fast_failed_eval_stops_batch() -> None:
    stop = FailFastStop("drain")
    executor = FakeExecutor()
    scheduler = JobScheduler(executor, [SYSTEM], fail_fast=stop)
    bad = NixEvalJobError(error="boom", attr="bad", attr_path=["bad"])
    result = await scheduler.run([mk_job("a"), bad, mk_job("b", deps=["a"])])
    assert stop.trigger_attr == "bad"
    assert executor.built == []
    statuses = by_attr(result)
    assert statuses["bad"] == AttributeStatus.failed_eval
    assert statuses["a"] == AttributeStatus.cancelled
    assert statuses["b"] == AttributeStatus.cancelled
    assert {r.error for r in result.results if r.attr != "bad"} == {
        "stopped: fail-fast after bad"
    }


async def test_fail_fast_late_batch_settles_cancelled() -> None:
    stop = FailFastStop("drain")
    bad_reported = asyncio.Event()

    async def on_result(result: AttributeResult) -> None:
        if result.attr == "bad":
            bad_reported.set()

    executor = FakeExecutor({"bad": BuildOutcome.failure})
    scheduler = JobScheduler(executor, [SYSTEM], on_result=on_result, fail_fast=stop)
    queue: asyncio.Queue[list[NixEvalJob] | None] = asyncio.Queue()
    await queue.put([mk_job("bad")])
    run_task = asyncio.create_task(scheduler.run_incremental(queue))
    await asyncio.wait_for(bad_reported.wait(), timeout=5)
    late_err = NixEvalJobError(error="boom", attr="late-err", attr_path=["late-err"])
    await queue.put(
        [
            mk_job("late"),
            mk_job("late-dep", deps=["bad"]),
            late_err,
            mk_job("foreign", system="riscv64-linux"),
        ]
    )
    await queue.put(None)
    result = await asyncio.wait_for(run_task, timeout=5)
    assert executor.built == ["bad"]
    statuses = by_attr(result)
    assert statuses["late"] == AttributeStatus.cancelled
    assert statuses["late-dep"] == AttributeStatus.cancelled
    # A late eval error is still reported as one but does not re-fire.
    assert statuses["late-err"] == AttributeStatus.failed_eval
    assert "foreign" not in statuses
    assert stop.trigger_attr == "bad"
    errors = {r.attr: r.error for r in result.results}
    assert errors["late"] == "stopped: fail-fast after bad"


async def test_fail_fast_only_first_failure_fires() -> None:
    stop = FailFastStop("cancel")

    class StopAwareExecutor(FakeExecutor):
        async def build(self, job: NixEvalJobSuccess) -> BuildOutcome:
            if job.attr == "slow":
                # What the executor does with stop_running in cancel mode.
                await stop.stop_running.wait()
                return BuildOutcome.cancelled
            if job.attr == "bad2":
                # Still running when bad1 fires the stop.
                await stop.stop_queued.wait()
            return await super().build(job)

    executor = StopAwareExecutor(
        {"bad1": BuildOutcome.failure, "bad2": BuildOutcome.failure}
    )
    scheduler = JobScheduler(executor, [SYSTEM], fail_fast=stop)
    jobs: list[NixEvalJob] = [
        mk_job("bad1"),
        mk_job("bad2"),
        mk_job("slow"),
        mk_job("child", deps=["bad1"]),
        mk_job("x", deps=["slow"]),
    ]
    result = await asyncio.wait_for(scheduler.run(jobs), timeout=5)
    assert stop.trigger_attr == "bad1"
    statuses = by_attr(result)
    assert statuses["bad1"] == AttributeStatus.failed
    assert statuses["bad2"] == AttributeStatus.failed
    # A dependent of the trigger is a genuine dependency failure.
    assert statuses["child"] == AttributeStatus.dependency_failed
    assert statuses["x"] == AttributeStatus.cancelled
    # A build killed by the stop carries the stop reason.
    assert statuses["slow"] == AttributeStatus.cancelled
    errors = {r.attr: r.error for r in result.results}
    assert errors["slow"] == "stopped: fail-fast after bad1"
    assert errors["x"] == "stopped: fail-fast after bad1"


async def test_fail_fast_unset_keeps_building() -> None:
    executor, result = await _run_fail_fast(mk_job("bad"), BuildOutcome.failure)
    assert "x" in executor.built
    statuses = by_attr(result)
    assert statuses["bad"] == AttributeStatus.failed
    assert statuses["x"] == AttributeStatus.succeeded
    assert all(r.error is None for r in result.results)
