-- Instruct investigation agents to set blockedBy/blocks relationships on
-- follow-up tickets so the supervisor dispatches them in the correct order.

UPDATE stage_templates
SET prompt_template = 'Work on Linear ticket {ticket_id}. This is an investigation ticket. Produce a summary of findings as a Linear comment on the ticket. If you identify implementation work, create follow-up Linear tickets. Do not create a PR.

When creating multiple follow-up tickets where one depends on another, set blockedBy / blocks relationships between them using the Linear MCP save_issue tool. This ensures the supervisor dispatches them in the correct order. Do not just mention dependencies in the description — set them as actual Linear relations.'
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE name = 'investigation')
  AND executor_type = 'claude';
