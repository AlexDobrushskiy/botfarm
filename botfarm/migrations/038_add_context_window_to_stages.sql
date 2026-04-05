-- Add per-stage context window override (nullable INTEGER, tokens).
-- When NULL, falls back to DEFAULT_CONTEXT_WINDOW (200k tokens).
ALTER TABLE stage_templates ADD COLUMN context_window INTEGER;
