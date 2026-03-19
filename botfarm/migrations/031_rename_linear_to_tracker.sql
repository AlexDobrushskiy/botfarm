-- Migration 031: Rename Linear-specific column names to tracker-agnostic names.
-- Supports clean multi-bugtracker integration by removing Linear-specific naming.
-- SQLite supports ALTER TABLE ... RENAME COLUMN since 3.25.0.

-- ticket_history table
ALTER TABLE ticket_history RENAME COLUMN linear_uuid TO tracker_uuid;
ALTER TABLE ticket_history RENAME COLUMN linear_created_at TO tracker_created_at;
ALTER TABLE ticket_history RENAME COLUMN linear_updated_at TO tracker_updated_at;
ALTER TABLE ticket_history RENAME COLUMN linear_completed_at TO tracker_completed_at;

-- dispatch_state table (capacity tracking columns from migration 016)
ALTER TABLE dispatch_state RENAME COLUMN linear_issue_count TO tracker_issue_count;
ALTER TABLE dispatch_state RENAME COLUMN linear_issue_limit TO tracker_issue_limit;
ALTER TABLE dispatch_state RENAME COLUMN linear_capacity_checked_at TO tracker_capacity_checked_at;
ALTER TABLE dispatch_state RENAME COLUMN linear_capacity_by_project TO tracker_capacity_by_project;

-- cleanup_batch_items table
ALTER TABLE cleanup_batch_items RENAME COLUMN linear_uuid TO tracker_uuid;
