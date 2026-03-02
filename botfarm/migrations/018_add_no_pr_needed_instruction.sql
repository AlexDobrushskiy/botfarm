-- Add NO_PR_NEEDED instruction to the default implementation pipeline's
-- implement stage prompt template so the agent knows to signal when work
-- is already present on main and no PR is required.

UPDATE stage_templates
SET prompt_template = 'Work on Linear ticket {ticket_id}. Follow the Linear Tickets workflow in CLAUDE.md. Complete all steps through PR creation. Do not stop until the PR is created. If the work described in the ticket is already fully implemented on main (e.g. delivered by another PR), verify all acceptance criteria are met, then output NO_PR_NEEDED: <explanation> as your final message. Do not create a branch or PR in that case.'
WHERE name = 'implement'
  AND pipeline_id = (SELECT id FROM pipeline_templates WHERE is_default = 1)
  AND executor_type = 'claude';
