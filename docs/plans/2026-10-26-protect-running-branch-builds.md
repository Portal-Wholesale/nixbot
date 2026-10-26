# Protect running branch builds from supersede

Status: plan, not started. Branch `nick/protect-running-branch-builds`,
based on the deployed `nick/attribute-branches-upstream-3c432d5` (10dd4d9).

## Problem

A newer push to a branch cancels that branch's in-flight build
(`CancellationManager.register` → `_release_context(old, …, "superseded")`).
For Portal-Wholesale/cms `main` that means the full suite almost never
finishes:

- 2026-10-26 05:34Z–22:10Z: about 49 post-merge `main` builds were cancelled.
  The only success (7718) was a 0-minute tree-hash reuse. In the last 2.5
  hours, 11 of 13 `main` builds were cancelled.
- Some were cancelled late. 7726 was cancelled at 27.7 min, after
  `web-integration-1..3` had passed, with `portal-test-partition-1..4` still
  running. 7755 was cancelled at 15.3 min.
- The long pole is always `portal-test-partition-*` and `web-integration-*`
  (25–30 min). Compile, lint and unit checks finish in under 10 min. Every
  merge changes those inputs, so Nix caching does not help.
- PR builds use `fastChecks`, so the full Portal suite runs only on `main`.
  Cancelling those builds means the full suite is effectively never run.

## Goal

For configured branches, a build that has run longer than a grace period
is never superseded. Newer pushes wait in one slot per branch: a newer
push replaces the waiting one, and when the running build finishes, the
waiting commit runs.

Outcome for `main` with a 5-minute grace period: a full result about every
30 minutes, the newest commit is always the next one tested, and at most one
full `main` build runs (plus one waiting).

## Non-goals

- No change for pull requests, merge-queue branches, tags, or branches
  without a rule. Their supersede behaviour stays exactly as it is.
- Protection only blocks supersede. PR close, branch deletion, and manual
  cancels still cancel a protected build.
- No progress-based (percent of attributes) heuristic. Attribute counts
  say nothing about the long pole (7726 was 12/19 done with 28 min spent).
- No new global (`Config`) setting. This is per-repo `nixbot.toml`.
- No change to the GitHub status wording or state of a superseded build,
  except the new "waiting" status (see U5).

## Behaviour

For a push event on branch B whose `nixbot.toml` has a matching rule:

| Running build for B | Incoming push | Result |
|---|---|---|
| none | X | X runs (today's behaviour) |
| R, running < grace | X | R superseded, X runs (today's behaviour) |
| R, running ≥ grace | X | R keeps running; X is **deferred** (build row `pending`, status "waiting for build #R") |
| R protected, D deferred | X | D cancelled ("superseded"), X becomes the deferred one |
| R protected, D deferred | X is an ancestor of D or R | X ignored as stale (today's ancestry rule, extended to D) |
| R finishes (any terminal state) | — | D is released and runs; its own grace period starts then |
| B deleted | — | R cancelled (today), D cancelled and its slot dropped |

The same tree under the same attribute stays a DUPLICATE (shared build, no
deferral), as today.

## Config

`nixbot.toml`, alongside `attribute_branches`:

```toml
# Running builds of matching branches are not superseded once they have
# run `grace_seconds`; newer pushes wait and only the newest runs next.
[[protect_running_branches]]
match = "main"
grace_seconds = 300
```

`RepoBranchConfig.protect_grace(branch) -> int | None`: first rule whose glob
matches wins; `None` means no protection. Push events only. For a push, the
build worktree is the branch itself, so the rule comes from the branch's own
config, the same way `attribute` is read today. Pull requests never consult
it.

cms change, made after the fork ships: add the block above to
`nixbot.toml` in Portal-Wholesale/cms.

## Design

### Decision point: `CancellationManager.register`

- `InFlightBuild` gains `started_at: datetime` (from `builds.started_at`;
  falls back to registration time when unset). Recovery re-registration
  passes the DB value so a restart does not reset the clock.
- `register(...)` gains `protect_grace: timedelta | None` and `now`.
  When the context's old build is not a DUPLICATE, not STALE, and
  `now - old.started_at >= protect_grace`, it returns a new
  `RegisterOutcome.DEFERRED` and does **not** release the old context.
- The canceller stays in-memory and synchronous. It does not own the
  waiting slot; it only answers "protected?".

### Waiting slot: `deferred_changes` table

Fork-only migration `9003_deferred_changes.sql` (fork numbering after
`9002_build_attribute.sql`):

```sql
CREATE TABLE deferred_changes (
    project_id BIGINT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    context_key TEXT NOT NULL,
    build_id BIGINT NOT NULL REFERENCES builds (id) ON DELETE CASCADE,
    blocked_by_build_id BIGINT NOT NULL REFERENCES builds (id) ON DELETE CASCADE,
    change JSONB NOT NULL,          -- dataclasses.asdict(ChangeRequest)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, context_key)
);
```

Queries (`queries/deferred.sql`, sqlc → `db_gen/deferred.py`):

- `UpsertDeferredChange :one`: insert or replace for the context, returning
  the replaced row's `build_id` so the caller can cancel it. One statement,
  so two racing pushes cannot both survive.
- `TakeDeferredChange :one`: `DELETE … WHERE project_id = $1 AND
  context_key = $2 RETURNING *`.
- `DeferredBuildIds :many`: used by recovery.
- `DeleteDeferredForBranch :many`: branch deletion, returns build ids.

Why a table, not in-memory state: a crash or deploy while `main` is
protected must not lose the newest commit, and must not let recovery
start the deferred build next to the protected one.

### `Orchestrator._dispatch_build`

Today: `rebuild = created or (not in_flight and status == CANCELLED)`, then
`register`, then run or attach.

New:

1. Resolve `protect_grace` from `branch_config` (push events only).
2. Extend the stale check: also compare the incoming commit against the
   deferred commit for the context (one extra `is_ancestor` call, only
   when a deferred row exists).
3. On `DEFERRED`:
   - `UpsertDeferredChange` with the event's `ChangeRequest` and this
     build's id. If it replaced a row, set that build `cancelled` and post
     `build_finished(... CANCELLED ...)` exactly as the STALE branch does.
   - Post the new "waiting" status for this build (U5).
   - `release_run(build.id_)` and return. The worktree is removed by the
     caller as usual; the released run checks out again.
4. A released change carries `ChangeRequest.released_from_deferral = True`
   (new field, default `False`). `_dispatch_build` treats it as a rebuild
   of the existing `pending` row:
   `rebuild = created or released or (not in_flight and status == CANCELLED)`.
   The flag lives in the `work_queue` payload, so it survives a crash
   between release and run.

### Release on completion

Builds stop being tracked in three places: `_dispatch_build`'s `finally`,
`rerun_exec`'s `finally` (restarts and crash recovery), and
`build_reuse.reuse_terminal_build`. Replace the bare
`canceller.complete(build_id)` in the first two with
`Orchestrator.finish_tracking(build_id)`, which:

1. calls `canceller.complete(build_id)`, and
2. for each context the build held, runs `TakeDeferredChange` and, if a row
   exists and its `blocked_by_build_id` is this build, enqueues the stored
   change with `released_from_deferral=True` through the service's
   `_submit_change` (new orchestrator → sink callback; the orchestrator does
   not enqueue work today).

`reuse_terminal_build` never holds a running context, so it stays as is.

### Recovery

- `FindUnfinishedBuilds` excludes builds in `deferred_changes`, so a
  restart never resumes a deferred build next to its blocker.
- After `recover_unfinished_builds`, sweep deferred rows whose
  `blocked_by_build_id` is terminal or not resumable and release them. This
  covers a blocker that finished or was cleaned up while the service was
  down.
- `reconcile.py` needs no change: the deferred commit has a build row, so
  it counts as built, and the deferred table, not reconcile, owns running
  it.

### Branch deletion

`_submit_branch_deleted` also runs `DeleteDeferredForBranch` and cancels the
returned builds (status + report), next to the existing `cancel_branch`
and `supersede_pending_branch_changes`.

### Status reporting

New `StatusReporter.build_deferred(event, build, blocked_by)`: posts
`<prefix>/nix-build` as `pending` with "waiting for build #<number>". A
deferred build that is replaced keeps today's superseded wording
("build cancelled (superseded)", state `error`).

## Implementation units

- **U1 Config.** `ProtectRunningRule` + `protect_running_branches` on
  `RepoBranchConfig`, `protect_grace(branch)`. Tests in
  `test_repo_config.py`: glob match, first match wins, default none,
  invalid value falls back to defaults like other fields.
- **U2 Canceller.** `started_at` on `InFlightBuild`,
  `RegisterOutcome.DEFERRED`, grace decision in `register`. Tests in
  `test_canceller.py`: under grace supersedes; at/over grace defers and does
  not set the old cancel event; DUPLICATE and STALE unchanged with
  protection on; re-registration keeps `started_at`; `cancel_branch` still
  cancels a protected build.
- **U3 Storage.** Migration 9003, `queries/deferred.sql`, regenerated
  `db_gen` (the `sqlc-generated` check must pass), `FindUnfinishedBuilds`
  exclusion.
- **U4 Orchestrator flow.** `protect_grace` plumbing in `_dispatch_build`,
  deferral, replace-and-cancel, extended stale check,
  `released_from_deferral`, `finish_tracking`, the release callback into
  the service, branch-deletion cleanup, post-recovery sweep. Tests in
  `test_orchestrator.py` / `test_races.py`:
  - protected R + push X → X pending/deferred, R not cancelled, one build
    running;
  - R protected, D deferred, push X → D cancelled and reported, X deferred;
  - R finishes (success, failure, manual cancel) → D runs, and its grace
    starts from its own `started_at`;
  - out-of-order redelivery of an older commit while D is deferred → ignored;
  - two concurrent pushes while protected → exactly one deferred row
    survives;
  - branch deleted while R protected and D deferred → both cancelled, row
    gone;
  - crash with R protected and D deferred → recovery resumes R only, D runs
    after R; crash between release and run → D still runs once;
  - PR events on a protected base branch → unchanged supersede.
- **U5 Status.** `build_deferred` on the reporter and every reporter
  implementation (GitHub, Gitea, GitLab, the test fake). Test in
  `test_status.py`.
- **U6 Docs.** A `protect_running_branches` section wherever
  `attribute_branches` is documented in `README.md` / `docs/`.

## Validation

From the worktree:

```bash
env -u NIXBOT_URL -u NIXBOT_TOKEN nix develop -c python -m pytest nixbot/tests -q
nix build .#checks.x86_64-linux.{sqlc-generated,treefmt,nixbot-tests}
```

(`nix flake check` is broken on this pin; build the named checks. Use
`nix fmt`, not `flake-fmt`. Do not run two test suites at once.)

## Rollout

1. Fork PR on nickdichev/nixbot against
   `nick/attribute-branches-upstream-3c432d5`; merge = fast-forward that
   branch.
2. Bump clan's `nixbot` input (flake.lock node `nixbot_3`) and deploy
   `netc-man-staging-private-app-1` from a clan worktree on origin/main;
   check `git status vars/` afterwards.
3. With no `protect_running_branches` rule, behaviour is unchanged, so the
   deploy is safe on its own.
4. cms PR adding the `main` rule. It takes effect on the first `main`
   push after that PR merges, since the rule is read from the branch's own
   config.
5. Watch for a day: `nbo build list --branch main` should show mostly
   succeeded/failed post-merge builds (no PR number) of 25–30 min, with
   cancelled ones short (< 5 min) or replaced while waiting.

Rollback: remove the rule from cms `nixbot.toml`. No fork revert is needed.

## Done when

- U1–U6 are merged on the fork, the named checks pass, and the fork is
  deployed.
- The cms rule is merged and a day of `main` builds shows full results
  roughly every 30 minutes under normal merge traffic.

## Open questions

- Should replaced deferred builds post `error` (as superseded builds do
  today) or a neutral state? Unchanged here; a neutral state needs a
  forge-specific mapping and is a separate change.
- Effects (`effects_branches = ["main", "production"]`) now run for every
  completed protected `main` build instead of rarely, one after another and
  never overlapping. In cms (`nix/herculesCI.nix`) the `main` effects are
  the failed-build Olive notification and the onPush ECR image push. Images
  are tagged by version, so a repeated version skips the push. Olive will
  now see every red `main` build, which is the point, but expect more
  notifications at first.
