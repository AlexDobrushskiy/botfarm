-- Migration 027: Add codex_usage_snapshots table.
--
-- Stores periodic snapshots of OpenAI org-level spending
-- polled from the Costs API using an Admin API key.

CREATE TABLE codex_usage_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    daily_spend         REAL,
    monthly_spend       REAL,
    monthly_budget      REAL,
    budget_utilization   REAL,
    raw_json            TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
