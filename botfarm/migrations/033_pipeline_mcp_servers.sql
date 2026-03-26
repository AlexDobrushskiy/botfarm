-- Migration 033: Add mcp_servers JSON column to pipeline_templates.
-- Stores additional MCP server configurations that a pipeline needs (e.g. Playwright
-- for QA pipelines).  NULL means no extra servers.  Value is a JSON object of
-- {server_name: server_config} entries, merged into the base bugtracker MCP config
-- at pipeline start.

ALTER TABLE pipeline_templates ADD COLUMN mcp_servers TEXT DEFAULT NULL;
