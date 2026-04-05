-- Add per-stage context window override (nullable INTEGER, tokens).
-- When NULL, falls back to the model's max_input_tokens from available_models.
ALTER TABLE stage_templates ADD COLUMN context_window INTEGER;
