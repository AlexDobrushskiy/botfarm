CREATE TABLE slots (
    project             TEXT NOT NULL,
    slot_id             INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'free',
    ticket_id           TEXT,
    ticket_title        TEXT,
    branch              TEXT,
    pr_url              TEXT,
    stage               TEXT,
    stage_iteration     INTEGER NOT NULL DEFAULT 0,
    current_session_id  TEXT,
    started_at          TEXT,
    stage_started_at    TEXT,
    sigterm_sent_at     TEXT,
    pid                 INTEGER,
    interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
    resume_after        TEXT,
    stages_completed    TEXT,
    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY(project, slot_id)
);

CREATE TABLE dispatch_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    paused          INTEGER NOT NULL DEFAULT 0,
    pause_reason    TEXT,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
