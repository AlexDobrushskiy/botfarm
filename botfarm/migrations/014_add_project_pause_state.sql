-- Per-project pause state: allows pausing dispatch for individual projects
-- while letting running workers finish their current task.
CREATE TABLE IF NOT EXISTS project_pause_state (
    project TEXT PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    pause_reason TEXT,
    updated_at TEXT
);
