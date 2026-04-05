-- Add per-stage thinking mode override (nullable TEXT).
-- Valid values: 'enabled', 'adaptive', 'disabled'. NULL = inherit user default.
ALTER TABLE stage_templates ADD COLUMN thinking_mode TEXT;
