-- Migration 017: Create ticket_history table for local Linear ticket backup.
-- Stores full ticket content (description, comments, labels, dependencies)
-- so ticket details survive deletion from Linear.

CREATE TABLE IF NOT EXISTS ticket_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id           TEXT NOT NULL UNIQUE,
    linear_uuid         TEXT,
    title               TEXT NOT NULL,
    description         TEXT,
    status              TEXT,
    priority            INTEGER,
    url                 TEXT,
    assignee_name       TEXT,
    assignee_email      TEXT,
    creator_name        TEXT,
    project_name        TEXT,
    team_name           TEXT,
    estimate            REAL,
    due_date            TEXT,
    parent_id           TEXT,
    children_ids        TEXT DEFAULT '[]',
    blocked_by          TEXT DEFAULT '[]',
    blocks              TEXT DEFAULT '[]',
    labels              TEXT DEFAULT '[]',
    comments_json       TEXT DEFAULT '[]',
    pr_url              TEXT,
    branch_name         TEXT,
    linear_created_at   TEXT,
    linear_updated_at   TEXT,
    linear_completed_at TEXT,
    captured_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    capture_source      TEXT NOT NULL,
    raw_json            TEXT,
    deleted_from_linear INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ticket_history_ticket_id ON ticket_history(ticket_id);
CREATE INDEX IF NOT EXISTS idx_ticket_history_project ON ticket_history(project_name);
CREATE INDEX IF NOT EXISTS idx_ticket_history_status ON ticket_history(status);
CREATE INDEX IF NOT EXISTS idx_ticket_history_captured_at ON ticket_history(captured_at);
