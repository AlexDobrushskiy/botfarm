ALTER TABLE dispatch_state ADD COLUMN linear_issue_count INTEGER;
ALTER TABLE dispatch_state ADD COLUMN linear_issue_limit INTEGER DEFAULT 250;
ALTER TABLE dispatch_state ADD COLUMN linear_capacity_checked_at TEXT;
ALTER TABLE dispatch_state ADD COLUMN linear_capacity_by_project TEXT;
