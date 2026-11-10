# Fail-fast branch builds

Status: plan, not started. Branch `nick/fail-fast-branches-plan`, based on
`portal/main` (e015904).

## Problem

On branches like cms `main`, a single failed attribute already means the build
will not be promoted: deploy gating needs a green build. Even so, the build
keeps evaluating and running every other attribute for another 25–30 minutes
(`portal-test-partition-*`, `web-integration-*`). That uses eval and attribute
slots and tribuchet workers that PR CI and the next `main` build are waiting
for. The person who merged also finds out late that the merge was bad.

## Goal

A repository can mark branches as fail-fast. After the first real failure, the
build stops and reports **failed** right away. There are two modes:

- `cancel`: stop everything. The evaluation and every running and queued
  attribute build are cancelled.
- `drain`: stop the evaluation and every attribute still waiting for a slot.
  Attribute builds already running finish and report, so you see a few more
  failures without starting new work.

## Non-goals

- Push events only. A pull request is unaffected even when its base branch
  matches.
- No change to supersede, `protect_running_branches`, manual cancel, or branch
  deletion.
- No new attribute status and no migration. Stopped attributes are `cancelled`
  with an explanatory error.
- Restarts (`restart`, rebuild-failed) do not apply fail-fast. Restarting is an
  explicit request for full results.
- No "N failures before stopping" threshold.

## Config

In `nixbot.toml`, alongside `protect_running_branches`:

```toml
# After the first failure, stop matching push builds. "cancel" stops
# everything; "drain" lets running attribute builds finish.
[[fail_fast_branches]]
match = "main"
mode = "cancel"
```

`BranchConfig.fail_fast(branch) -> FailFastMode | None`: the first rule whose
glob matches wins, and `None` means no fail-fast. It is read from the push's own
worktree config, the same way `protect_grace` is. A repo-level setting is safe
here because it only ever shortens that repo's own builds.

cms change, made after the fork ships: add the block above to `nixbot.toml` in
Portal-Wholesale/cms.

## Behaviour

**What triggers a stop:** the first attribute that settles as `failed`,
`failed_eval`, or `cached_failure`.

- `requireFailure` attributes count only through their final status, which
  `outcome_status` already inverts.
- `ignored_failure` and `post_build_failure` do not trigger a stop. The
  derivation built; only an upload or post-build step failed.
- Internal errors (`failure_no_cache`, "internal error, see service logs") do
  not trigger a stop. The attribute still settles `failed`.
- `dependency_failed` and `cancelled` do not trigger. They are consequences of a
  stop or failure, not a new failure.

| State when the stop fires        | `cancel`                                   | `drain`                |
| -------------------------------- | ------------------------------------------ | ---------------------- |
| Evaluation still running         | stopped; unevaluated attributes get no row | same                   |
| Pending jobs (dependencies)      | settled `cancelled`                        | same                   |
| Started, waiting for a slot      | `cancelled`, never takes a slot            | same                   |
| Running `nix build`              | process group killed, `cancelled`          | runs to its own result |
| Late eval batch (already queued) | settled `cancelled`                        | same                   |
| Build aggregate                  | `FAILED` (one attribute failed)            | `FAILED`               |

Every stopped attribute's error reads `stopped: fail-fast after <attr>`, so it
is not confused with a supersede. The build summary names the attribute that
triggered the stop.

## Design

### A stop object shared by the scheduler and the executor

```python
@dataclass
class FailFastStop:
    mode: FailFastMode
    # Set by the first triggering failure.
    trigger_attr: str | None = None
    # Both modes: jobs waiting for a slot give up.
    stop_queued: asyncio.Event = field(default_factory=asyncio.Event)
    # Cancel mode only: running builds are killed.
    stop_running: asyncio.Event = field(default_factory=asyncio.Event)

    def fire(self, attr: str) -> bool: ...  # first call wins
    @property
    def reason(self) -> str: ...  # "stopped: fail-fast after <attr>"
```

Fail-fast deliberately does **not** reuse the build's `cancel_event`. That event
means the build was superseded or cancelled: the eval race in `build_run.py:456`
settles it as `CANCELLED` through `_settle_aborted`, and it has to end as
`FAILED`.

### `JobScheduler`

- The constructor gains `fail_fast: FailFastStop | None = None`.
- `_finish_job`, `_partition_jobs` (for `failed_eval`) and `_classify` (for
  `cached_failure`) call `fail_fast.fire(attr)` when the status triggers. The
  first fire:
  - settles every `state.pending` job as `cancelled` with the stop reason and
    adds them to `failed_drvs` and `cancelled_drvs`, so late dependents settle
    as `cancelled` as well;
  - sets `stop_queued`, and in `cancel` mode also `stop_running`.
- `_ingest`: once stopped, every fresh job settles as `cancelled` with the
  reason (the eval may still deliver a batch that was already in flight).
- `_dispatch_ready` starts nothing after a stop.

### Executor

- `NixBuildExecutor.build_attribute` gains `stop_queued: asyncio.Event | None`,
  raced against `queue.acquire` next to `cancel_event` (`executor.py:839`). If
  it wins, the attribute returns `cancelled` without a slot. Once a slot is
  acquired, `stop_queued` is ignored, which is what makes `drain` work.
- `_OrchestratorExecutor` passes `stop.stop_queued`, and mirrors
  `stop.stop_running` into the per-attribute cancel event the same way it
  mirrors the build `cancel_event` today (`build_run.py:741`).
- When an attribute ends `cancelled` and the stop has fired, the completion row
  written by `_OrchestratorExecutor` carries `stop.reason` as its error.

### Stopping the evaluation

`build_run.py` runs the eval and `build_attributes` concurrently and races the
eval against `cancel_wait`. Add the stop as a third wait. If it fires while the
eval is running:

- reap only `eval_task` (`nix-eval-jobs` is killed and the eval slot freed);
- put the `None` sentinel on `jobs_queue` so `run_incremental` finishes;
- let `build_task` end normally: `aggregate_build` sees a failed attribute and
  returns `FAILED`, and reporting and `after_build` run as usual;
- skip the eval's own failure handling for this case. The eval was stopped on
  purpose, so it is not an eval error.

### Where the rule is resolved

In the change path where `branch_config` is already loaded, for push events only
and not for restarts. That path builds a `FailFastStop` and passes it to
`build_attributes` and on to `JobScheduler`. `build_attributes` gains a
`fail_fast` keyword argument that defaults to `None`, so reruns and recovery
pass nothing.

### What already works without changes

- `aggregate_build` returns `FAILED` when any attribute failed, even if the rest
  are `cancelled`.
- Cancelled attributes never enter the failed-build cache.
- `finish_tracking` releases a deferred push as soon as the protected build
  ends, so a fail-fast stop on a protected `main` build starts the waiting
  commit early.
- Tribuchet drops a queued hub job only when every attached client is gone, so a
  derivation that a PR build also needs keeps building.

## Implementation units

- **U1. Repo config.** `FailFastRule` (`match`,
  `mode: Literal["cancel", "drain"]`), `BranchConfig.fail_fast_branches` and
  `fail_fast(branch)`, with tests in `test_repo_config.py`: first match wins, no
  match, and invalid mode falls back to defaults like any invalid config.
- **U2. Scheduler.** `FailFastStop` and the `JobScheduler` changes. Tests in
  `test_build_scheduler.py` with a fake executor:
  - each triggering status fires the stop; each non-triggering one doesn't;
  - pending jobs and late batches settle `cancelled` with the reason;
  - only the first failure fires;
  - with no stop configured, behaviour is unchanged.
- **U3. Executor.** The `stop_queued` race and the `stop_running` mirror. Tests
  in `test_executor.py`:
  - drain lets an acquired build finish and cancels a waiter, with no slot leak;
  - cancel kills a running build;
  - a stop that fires in the same turn as a slot grant gives the slot back.
- **U4. Build wiring.** Resolve the rule (push events, not restarts), stop the
  eval, and finish as `FAILED`. Integration tests:
  - a failure during the eval stops the eval and the build ends `FAILED`;
  - drain keeps a running attribute's result;
  - a restart of the same build ignores fail-fast;
  - a protected build's deferred push is released at the stop.
- **U5. Reporting.** The build summary line naming the trigger, and the stopped
  attributes' description on the forge statuses. Check that no "superseded"
  wording appears.
- **U6. Docs.** Document `fail_fast_branches` in `README.md` next to
  `protect_running_branches`.
- **U7. Rollout.** Fast-forward `portal/main`, then a clan pin bump, then the
  cms `nixbot.toml` rule.

## Done when

- A cms `main` build with a failing attribute ends `FAILED` within seconds of
  that failure (`cancel`) or once its running attributes finish (`drain`).
- Its slots and its eval slot are free for other builds, and nothing is cached
  as a failure except the real failure.
- PR builds, restarts, and branches without a rule behave exactly as today.

## Open questions

- Which mode to start cms `main` with. `cancel` frees the most capacity; `drain`
  shows more failures when a merge breaks several suites. The suggestion is
  `cancel`, switching to `drain` if fixing one failure at a time hurts.
- Should a flake-retry policy (rerun the triggering attribute once before
  stopping) be part of this? The suggestion is no. The executor's transient
  retry already covers infrastructure errors, and a genuinely flaky test blocked
  promotion before this change too.
