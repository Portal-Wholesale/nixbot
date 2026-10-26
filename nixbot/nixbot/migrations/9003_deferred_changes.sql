-- One waiting push per protected branch context: a newer push while the
-- context's running build is protected from supersede. The change is the
-- event's ChangeRequest, re-enqueued once the blocking build finishes.
-- replaced_build_id is the deferred build an upsert displaced, so the
-- caller can cancel it; it is only meaningful in the upsert's result.
CREATE TABLE deferred_changes (
    project_id BIGINT NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    context_key TEXT NOT NULL,
    build_id BIGINT NOT NULL REFERENCES builds (id) ON DELETE CASCADE,
    blocked_by_build_id BIGINT NOT NULL REFERENCES builds (id) ON DELETE CASCADE,
    change JSONB NOT NULL,
    replaced_build_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, context_key)
);
CREATE INDEX deferred_changes_build_idx ON deferred_changes (build_id);
