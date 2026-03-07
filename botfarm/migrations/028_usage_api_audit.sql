-- Migration 028: Add usage API audit log tables.
-- Tracks every usage API call attempt and per-key session lifecycle.

CREATE TABLE usage_api_calls (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL,
    token_fingerprint TEXT,
    status_code      INTEGER,
    success          INTEGER NOT NULL DEFAULT 0,
    error_type       TEXT,
    error_detail     TEXT,
    response_time_ms REAL,
    retry_after      TEXT,
    caller           TEXT
);

CREATE INDEX idx_usage_api_calls_created_at ON usage_api_calls(created_at);
CREATE INDEX idx_usage_api_calls_token ON usage_api_calls(token_fingerprint);
CREATE INDEX idx_usage_api_calls_success ON usage_api_calls(success);

CREATE TABLE usage_api_key_sessions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    token_fingerprint     TEXT NOT NULL UNIQUE,
    first_seen_at         TEXT NOT NULL,
    last_success_at       TEXT,
    first_error_at        TEXT,
    last_error_at         TEXT,
    consecutive_errors    INTEGER NOT NULL DEFAULT 0,
    total_errors          INTEGER NOT NULL DEFAULT 0,
    total_successes       INTEGER NOT NULL DEFAULT 0,
    status                TEXT NOT NULL DEFAULT 'active',
    blocked_at            TEXT,
    unblocked_at          TEXT,
    replaced_at           TEXT,
    block_duration_seconds REAL,
    created_at            TEXT NOT NULL
);

CREATE INDEX idx_usage_api_key_sessions_status ON usage_api_key_sessions(status);
