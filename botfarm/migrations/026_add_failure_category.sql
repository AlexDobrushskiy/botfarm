-- Migration 026: Add failure_category column to tasks table.
--
-- Classifies pipeline failures into categories (env_missing_runtime,
-- env_missing_package, env_missing_service, env_missing_config,
-- code_failure, limit_hit, timeout) so the supervisor can differentiate
-- behavior and the dashboard can display/filter by category.

ALTER TABLE tasks ADD COLUMN failure_category TEXT;
