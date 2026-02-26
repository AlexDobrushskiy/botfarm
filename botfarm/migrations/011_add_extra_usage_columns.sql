-- Extra usage tracking columns

-- usage_snapshots: store extra usage data from the Anthropic API
ALTER TABLE usage_snapshots ADD COLUMN extra_usage_enabled INTEGER DEFAULT 0;
ALTER TABLE usage_snapshots ADD COLUMN extra_usage_monthly_limit REAL;
ALTER TABLE usage_snapshots ADD COLUMN extra_usage_used_credits REAL;
ALTER TABLE usage_snapshots ADD COLUMN extra_usage_utilization REAL;

-- tasks: whether the task was started while extra usage was active
ALTER TABLE tasks ADD COLUMN started_on_extra_usage INTEGER DEFAULT 0;

-- stage_runs: whether the stage ran while extra usage was active
ALTER TABLE stage_runs ADD COLUMN on_extra_usage INTEGER DEFAULT 0;
