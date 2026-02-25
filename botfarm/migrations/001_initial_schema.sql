CREATE TABLE tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id       TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    project         TEXT NOT NULL,
    slot            INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at      TEXT,
    completed_at    TEXT,
    cost_usd        REAL NOT NULL DEFAULT 0.0,
    turns           INTEGER NOT NULL DEFAULT 0,
    review_iterations INTEGER NOT NULL DEFAULT 0,
    comments        TEXT NOT NULL DEFAULT '',
    limit_interruptions INTEGER NOT NULL DEFAULT 0,
    failure_reason  TEXT
);

CREATE TABLE stage_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             INTEGER NOT NULL REFERENCES tasks(id),
    stage               TEXT NOT NULL,
    iteration           INTEGER NOT NULL DEFAULT 1,
    session_id          TEXT,
    turns               INTEGER NOT NULL DEFAULT 0,
    duration_seconds    REAL,
    cost_usd            REAL NOT NULL DEFAULT 0.0,
    exit_subtype        TEXT,
    was_limit_restart   INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE usage_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    utilization_5h      REAL,
    utilization_7d      REAL,
    resets_at           TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER REFERENCES tasks(id),
    event_type  TEXT NOT NULL,
    detail      TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_stage_runs_task_id ON stage_runs(task_id);
CREATE INDEX idx_task_events_task_id ON task_events(task_id);
CREATE INDEX idx_task_events_type ON task_events(event_type);
CREATE INDEX idx_tasks_ticket_id ON tasks(ticket_id);
CREATE INDEX idx_tasks_status ON tasks(status);
