-- Track which pipeline template was used for each task and stage run,
-- enabling A/B comparison queries across pipeline variants.
ALTER TABLE tasks ADD COLUMN pipeline_id INTEGER REFERENCES pipeline_templates(id);
ALTER TABLE stage_runs ADD COLUMN pipeline_id INTEGER REFERENCES pipeline_templates(id);
