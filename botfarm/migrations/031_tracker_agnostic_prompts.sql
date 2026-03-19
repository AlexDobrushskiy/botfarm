-- Migration 031: Make stage template prompts tracker-agnostic.
--
-- Replace hardcoded "Linear" references with {bugtracker_type} template variable
-- so agents work correctly with any configured bugtracker (Linear, Jira, etc.).
--
-- Uses WHERE guards on old text to avoid clobbering user-customized templates.

-- Implementation pipeline: implement stage
-- "Work on Linear ticket" → "Work on {bugtracker_type} ticket"
UPDATE stage_templates
SET prompt_template = REPLACE(prompt_template, 'Work on Linear ticket {ticket_id}', 'Work on {bugtracker_type} ticket {ticket_id}')
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE is_default = 1)
  AND executor_type = 'claude'
  AND prompt_template LIKE '%Work on Linear ticket {ticket_id}%';

-- Investigation pipeline: implement stage
-- Multiple replacements for Linear-specific references.
-- "Work on Linear ticket" → "Work on {bugtracker_type} ticket"
UPDATE stage_templates
SET prompt_template = REPLACE(prompt_template, 'Work on Linear ticket {ticket_id}', 'Work on {bugtracker_type} ticket {ticket_id}')
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE name = 'investigation')
  AND executor_type = 'claude'
  AND prompt_template LIKE '%Work on Linear ticket {ticket_id}%';

-- "as a Linear comment on the ticket" → "as a comment on the ticket"
UPDATE stage_templates
SET prompt_template = REPLACE(prompt_template, 'as a Linear comment on the ticket', 'as a comment on the ticket')
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE name = 'investigation')
  AND executor_type = 'claude'
  AND prompt_template LIKE '%as a Linear comment on the ticket%';

-- "create follow-up Linear tickets" → "create follow-up tickets"
UPDATE stage_templates
SET prompt_template = REPLACE(prompt_template, 'create follow-up Linear tickets', 'create follow-up tickets')
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE name = 'investigation')
  AND executor_type = 'claude'
  AND prompt_template LIKE '%create follow-up Linear tickets%';

-- "using the Linear MCP save_issue tool" → "using the {bugtracker_type} API or MCP tools"
UPDATE stage_templates
SET prompt_template = REPLACE(prompt_template, 'using the Linear MCP save_issue tool', 'using the {bugtracker_type} API or MCP tools')
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE name = 'investigation')
  AND executor_type = 'claude'
  AND prompt_template LIKE '%using the Linear MCP save_issue tool%';

-- "set blockedBy / blocks relationships between them" → "set blocking relationships between tickets"
-- + "as actual Linear relations" → "as actual blocking relations"
UPDATE stage_templates
SET prompt_template = REPLACE(
    REPLACE(prompt_template,
        'set blockedBy / blocks relationships between them',
        'set blocking relationships between tickets'),
    'as actual Linear relations',
    'as actual blocking relations')
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE name = 'investigation')
  AND executor_type = 'claude'
  AND prompt_template LIKE '%set blockedBy / blocks relationships between them%';
