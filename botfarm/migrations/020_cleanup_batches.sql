-- Migration 020: Create cleanup batch tracking tables for bulk archive/delete.
-- Stores batch operation records and individual item results for undo support.

CREATE TABLE IF NOT EXISTS cleanup_batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL UNIQUE,
    action      TEXT NOT NULL,  -- 'archive' or 'delete'
    team_key    TEXT,
    project_name TEXT,
    total       INTEGER NOT NULL DEFAULT 0,
    succeeded   INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    skipped     INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS cleanup_batch_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES cleanup_batches(batch_id),
    linear_uuid TEXT NOT NULL,
    identifier  TEXT NOT NULL,
    action      TEXT NOT NULL,  -- 'archive', 'delete', or 'unarchive'
    success     INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_cleanup_batches_created_at ON cleanup_batches(created_at);
CREATE INDEX IF NOT EXISTS idx_cleanup_batch_items_batch_id ON cleanup_batch_items(batch_id);
