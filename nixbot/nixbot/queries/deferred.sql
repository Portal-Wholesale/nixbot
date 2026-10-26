-- Deferred pushes waiting on a protected running build (orchestrator.py).

-- name: UpsertDeferredChange :one
-- Insert or replace the context's waiting change in one statement, so
-- two racing pushes cannot both survive. The DO UPDATE SET reads the
-- locked current row, so replaced_build_id is the displaced build even
-- when a concurrent insert won the race; it is null for a fresh insert.
INSERT INTO deferred_changes (project_id, context_key, build_id,
                              blocked_by_build_id, change)
VALUES ($1, $2, $3, $4, sqlc.arg(change)::jsonb)
ON CONFLICT (project_id, context_key) DO UPDATE SET
    build_id = EXCLUDED.build_id,
    blocked_by_build_id = EXCLUDED.blocked_by_build_id,
    change = EXCLUDED.change,
    replaced_build_id = NULLIF(deferred_changes.build_id, EXCLUDED.build_id),
    created_at = now()
RETURNING replaced_build_id;

-- name: TakeDeferredChange :one
DELETE FROM deferred_changes WHERE project_id = $1 AND context_key = $2
RETURNING *;

-- name: DeferredBuildIds :many
SELECT build_id FROM deferred_changes ORDER BY build_id;

-- name: DeleteDeferredForBranch :many
-- Push contexts are keyed "branch/<name>" (canceller.context_key).
DELETE FROM deferred_changes
WHERE project_id = $1 AND context_key = 'branch/' || sqlc.arg(branch)::text
RETURNING build_id;

-- name: GetDeferredChange :one
SELECT * FROM deferred_changes WHERE project_id = $1 AND context_key = $2;

-- name: DeferredChangesBlockedBy :many
-- A finished build's waiting changes, in every context it held.
SELECT * FROM deferred_changes WHERE blocked_by_build_id = $1 ORDER BY build_id;

-- name: ListDeferredChanges :many
SELECT * FROM deferred_changes ORDER BY build_id;

-- name: DeleteDeferredChange :execrows
-- Only the released row: a newer push may have replaced it since.
DELETE FROM deferred_changes
WHERE project_id = $1 AND context_key = $2 AND build_id = $3;
