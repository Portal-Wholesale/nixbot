# Build priority

Status: plan, not started. Branch `nick/build-priority-plan`, based on
`portal/main` (e015904).

## Problem

Every build competes for the same capacity on equal terms. A post-merge
`main` build, which gates deploys, waits behind pull-request CI:

- **Eval slots.** `Orchestrator.eval_slots` is an `asyncio.Semaphore`
  (`orchestrator.py:180`), roughly first come first served. Clan sets
  `evalConcurrency = 2`. A cms eval is slow, so a `main` push that arrives
  behind two PR evals waits for both before it schedules any attribute.
- **Attribute slots.** `FairScheduler` (`executor.py:128`) is round-robin across
  builds, FIFO within one. Clan sets `buildConcurrency = 64`. With five PR
  builds waiting, `main` gets about a sixth of the slots that free up.
- **The tribuchet hub.** `HubState.take_job` (`hub/state.rs:270`) serves one
  FIFO `VecDeque<Job>` per matching system. Clan runs
  `boundBuildConcurrency = false` (measured 2026-11-06: 26 attributes made only
  9 hub builds), so part of the backlog waits at the hub, where nothing knows
  which build or branch a derivation belongs to.

The work-queue claim (`ClaimNextWorkItem`, `ORDER BY created_at`) is not a
wait point: `Service.work_loop` spawns every claimed item concurrently.

## Goal

Builds get a priority from the service configuration (for example `production`
and `main` push > merge queue > pull requests). Higher-priority builds take the
next free eval slot and the next free attribute slot. Lower priorities are
delayed, never starved, and nothing is preempted.

## Non-goals

- No preemption. A running eval or attribute build is never cancelled or
  paused to make room. That would repeat the supersede waste that
  `protect_running_branches` fixed.
- No per-repo `nixbot.toml` setting. Priority ranks builds across every
  repository the instance serves. Also, a pull request's `nixbot.toml` comes
  from the PR, so a repo-level setting would let a PR raise its own priority.
- No change to `boundBuildConcurrency`. Binding it would move the queue into
  nixbot but starves the burst scaler (2026-11-06 measurement).
- Without a `build_priority` config, behaviour does not change at all.
- Tribuchet hub ordering is out of scope until U7 shows it matters (see U8).
- No UI for editing priorities and no per-build manual override.

## Config

Service-level (`Config`), set by the clan module:

```python
class PriorityRule(BaseModel):
    # Glob over "forge/owner/name"; None matches every repository.
    repo: str | None = None
    # Branch glob. For a pull request it tests the base branch.
    match: str
    events: list[AttributeEvent] = ["push", "pull_request"]
    priority: int


class BuildPriorityConfig(BaseModel):
    # First matching rule wins; no match means priority 0.
    rules: list[PriorityRule] = []
    # The last `reserved_slots` attribute slots go only to priority > 0.
    reserved_slots: NonNegativeInt = 0
    # A waiting build gains +1 effective priority per `age_step_seconds`
    # waited. 0 disables ageing.
    age_step_seconds: NonNegativeInt = 900


class Config(BaseModel):
    ...
    build_priority: BuildPriorityConfig | None = None
```

`priority_for(config, event) -> int` is a pure function over `ChangeEvent`
(`repo`, `branch`, `pr_number`). Builds reconstructed through
`event_for_build` get the same answer, so recovery and reruns need no stored
column and no migration.

The clan setting we expect to start with:

```nix
buildPriority = {
  rules = [
    { match = "production"; events = [ "push" ]; priority = 20; }
    { match = "main"; events = [ "push" ]; priority = 10; }
    { match = "mergify/merge-queue/*"; events = [ "push" ]; priority = 5; }
  ];
  reservedSlots = 16;
  ageStepSeconds = 900;
};
```

## Design

### A slot interface, not an edit to `FairScheduler`

`FairScheduler` is upstream code, and `portal/main` is rebased onto upstream
release tags. It stays unchanged. A new class sits next to it:

```python
class SlotScheduler(Protocol):
    async def acquire(self, key: object) -> None: ...
    def release(self) -> None: ...


@dataclass(frozen=True)
class SlotKey:
    build_id: int
    priority: int = 0
```

- `NixBuildExecutor.queue` is typed `SlotScheduler` instead of `FairScheduler`.
  The executor already uses only `acquire(build_key)` and `release()`, and its
  cancel race around `acquire` (`executor.py:839`) is unchanged.
- `_OrchestratorExecutor` (`build_run.py:754`, the only `build_attribute` call
  site) passes `SlotKey(build.id_, priority)` instead of `build.id_`. `SlotKey`
  is hashable, so it works as a waiter-queue key in both schedulers, and
  `FairScheduler` ignores the priority field.
- `bootstrap.py:294` builds `FairPriorityScheduler` when
  `config.build_priority` is set and `FairScheduler` otherwise.

### `FairPriorityScheduler`

The same slot pool, with one round-robin rotation per priority class:

- Waiters are grouped by `SlotKey`, exactly as in `FairScheduler`. A key's
  **effective priority** is `key.priority + wait // age_step_seconds`, where
  `wait` is how long its oldest waiter has waited. It is computed at dispatch
  time, so no timers are needed.
- `_dispatch` grants the next slot to the highest effective class that has
  waiters, round-robin within that class. With every key at the same priority
  and no ageing, this is exactly `FairScheduler`.
- Reserved slots: while `active >= capacity - reserved_slots`, only keys whose
  *configured* priority is above 0 are granted. Ageing does not unlock
  reserved slots, so a PR backlog can never fill them.
- The fast path (`active < capacity and not waiters`) must check the
  reservation too. A priority-0 acquire must not take a reserved slot just
  because nobody is waiting.
- Cancellation semantics, including giving back a slot granted at the same
  time as the cancel, are copied from `FairScheduler`.

### `PrioritySemaphore` for eval slots

`Orchestrator.eval_slots` becomes a small priority semaphore with the same
waiter rules (effective priority with ageing, FIFO within a class, no
reservation). `_evaluate` (`build_run.py:366`) acquires with the build's
priority. Without `build_priority`, `Orchestrator` keeps `asyncio.Semaphore`.

### Tribuchet (U8, gated)

nixbot runs next to the hub. Before scheduling a build's attributes, nixbot
could send the hub `(output path → priority)` hints for the build's jobs and
their `needed_builds`, with a TTL. `take_job` would then pick the
highest-priority matching job instead of the first one. A derivation shared by
several builds takes the highest hint. This is a new API between two forks.
Before designing it, check whether the hub request identifies a derivation by
anything nixbot also knows (output paths, or the drv path through the scratch
outputs).

## Implementation units

- **U0. Baseline.** From the Nixbot API, measure over a day of cms builds: for
  `main` push builds, the time from `pending` to `evaluating` (eval-slot wait),
  and for attributes the time from row creation to `started_at` (slot wait,
  since `started_at` is set after acquisition). Split by whether PR builds
  overlapped. Record the numbers in this plan. This decides whether the eval
  stage or the attribute stage matters more.
- **U1. Config.** `PriorityRule`, `BuildPriorityConfig`, `Config.build_priority`,
  and `priority_for`, with tests: first match wins, `repo` glob, PR uses the base
  branch, tag pushes, and the default 0.
- **U2. Slot interface.** `SlotScheduler`, `SlotKey`, `NixBuildExecutor` typed
  against the protocol, and `_OrchestratorExecutor` passing `SlotKey`. No
  behaviour change; the existing `test_executor.py` passes unchanged.
- **U3. `FairPriorityScheduler`.** Run the existing `FairScheduler` tests against
  it with every key at priority 0 (a regression suite). Add tests for: higher
  class first; round-robin within a class; ageing promotes a starved key;
  reserved slots are refused to priority 0, including on the fast path; and
  cancellation while waiting and at grant time.
- **U4. `PrioritySemaphore`.** Wired into `Orchestrator.eval_slots` and
  `_evaluate`, with tests for ordering, ageing, and cancellation of a waiter.
- **U5. Bootstrap selection.** Pick the classes from `config.build_priority`.
  Add a test that a missing config keeps `FairScheduler` and `asyncio.Semaphore`.
- **U6. Clan module.** A `buildPriority` option on the nixbot service module,
  serialised into the service config. The pin bump and the rules above go in
  the same clan PR.
- **U7. Measure after deploy.** Repeat U0, and read the hub's `/status` snapshot
  and `tribuchet_system_builds{state="queued"}` while `main` and PR builds
  overlap. Decide whether U8 is needed.
- **U8. Tribuchet hints (only if U7 shows hub queueing delays `main`).** A
  separate plan in the tribuchet fork.

## Done when

- With `build_priority` set, a `main` push that arrives while PR evals are
  queued takes the next free eval slot, and its attributes take the next free
  attribute slots ahead of PR attributes.
- No PR build waits forever: ageing promotes it within the configured step.
- Without `build_priority`, scheduling is exactly what it is today (the
  existing tests pass unchanged).
- U7's numbers are recorded here.

## Open questions

- Initial values for `reserved_slots` and `age_step_seconds`. The numbers above
  are guesses until U0.
- Should the build page show a build's priority? It is cheap
  (`priority_for` on the stored event), but it is not needed to ship.
