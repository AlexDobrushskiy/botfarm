-- Add model and effort columns to stage_templates for per-stage override.
ALTER TABLE stage_templates ADD COLUMN model TEXT;
ALTER TABLE stage_templates ADD COLUMN effort TEXT;

-- Cache of available models from the Anthropic Models API.
CREATE TABLE IF NOT EXISTS available_models (
    id                TEXT PRIMARY KEY,          -- e.g. "claude-opus-4-6"
    display_name      TEXT NOT NULL,
    max_input_tokens  INTEGER NOT NULL,          -- context window
    max_output_tokens INTEGER NOT NULL,
    supported_efforts TEXT,                      -- JSON array, e.g. '["low","medium","high","max"]'
    executor_type     TEXT NOT NULL DEFAULT 'claude',  -- which adapter this model belongs to
    is_alias          INTEGER NOT NULL DEFAULT 0,      -- 1 if alias (e.g. "opus") vs dated ID
    fetched_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
