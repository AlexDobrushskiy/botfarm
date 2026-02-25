CREATE TABLE IF NOT EXISTS queue_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL,
    position INTEGER NOT NULL,
    ticket_id TEXT NOT NULL,
    ticket_title TEXT NOT NULL,
    priority INTEGER NOT NULL,
    sort_order REAL NOT NULL,
    url TEXT NOT NULL,
    snapshot_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_entries_project ON queue_entries(project);
