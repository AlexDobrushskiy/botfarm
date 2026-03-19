-- Migration 029: Replace codex_usage_snapshots with rate-limit based schema.
--
-- The old table stored dollar spending from the OpenAI Costs API.
-- The new table stores utilization percentages from the ChatGPT backend API,
-- matching the pattern used by the Claude usage poller.

DROP TABLE IF EXISTS codex_usage_snapshots;

CREATE TABLE codex_usage_snapshots (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_type                TEXT,
    primary_used_pct         REAL,
    primary_reset_at         TEXT,
    primary_window_seconds   INTEGER,
    secondary_used_pct       REAL,
    secondary_reset_at       TEXT,
    secondary_window_seconds INTEGER,
    rate_limit_allowed       INTEGER,
    raw_json                 TEXT,
    created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
