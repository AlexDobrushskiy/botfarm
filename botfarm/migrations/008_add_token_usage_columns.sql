ALTER TABLE stage_runs ADD COLUMN input_tokens INTEGER DEFAULT 0;
ALTER TABLE stage_runs ADD COLUMN output_tokens INTEGER DEFAULT 0;
ALTER TABLE stage_runs ADD COLUMN cache_read_input_tokens INTEGER DEFAULT 0;
ALTER TABLE stage_runs ADD COLUMN cache_creation_input_tokens INTEGER DEFAULT 0;
ALTER TABLE stage_runs ADD COLUMN total_cost_usd REAL DEFAULT 0.0;
ALTER TABLE stage_runs ADD COLUMN context_fill_pct REAL;
ALTER TABLE stage_runs ADD COLUMN model_usage_json TEXT;
