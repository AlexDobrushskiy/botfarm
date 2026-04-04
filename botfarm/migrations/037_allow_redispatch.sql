-- Allow multiple tasks per ticket_id for A/B pipeline comparison.
-- Drops the UNIQUE constraint on tasks.ticket_id so re-dispatch can
-- create additional task rows for the same ticket.

-- Disable FK checks while rebuilding the table (stage_runs and
-- task_events reference tasks.id).
PRAGMA foreign_keys = OFF;

CREATE TABLE tasks_new (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id             TEXT NOT NULL,
    title                 TEXT NOT NULL,
    project               TEXT NOT NULL,
    slot                  INTEGER NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending',
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at            TEXT,
    completed_at          TEXT,
    turns                 INTEGER NOT NULL DEFAULT 0,
    review_iterations     INTEGER NOT NULL DEFAULT 0,
    comments              TEXT NOT NULL DEFAULT '',
    limit_interruptions   INTEGER NOT NULL DEFAULT 0,
    failure_reason        TEXT,
    pr_url                TEXT,
    pipeline_stage        TEXT,
    review_state          TEXT,
    started_on_extra_usage INTEGER DEFAULT 0,
    merge_conflict_retries INTEGER DEFAULT 0,
    result_text           TEXT,
    failure_category      TEXT,
    pipeline_id           INTEGER REFERENCES pipeline_templates(id)
);

INSERT INTO tasks_new
SELECT * FROM tasks;

DROP TABLE tasks;
ALTER TABLE tasks_new RENAME TO tasks;

-- Recreate indexes (non-unique on ticket_id).
CREATE INDEX idx_tasks_ticket_id ON tasks(ticket_id);
CREATE INDEX idx_tasks_status ON tasks(status);

PRAGMA foreign_keys = ON;
