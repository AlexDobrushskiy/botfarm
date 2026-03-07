-- Prepend {prior_context} variable to implement stage prompt templates.
-- When prior_context is empty (fresh ticket), this renders as empty string.
-- When populated (retry), the context block appears before the implement instructions.
UPDATE stage_templates
SET prompt_template = '{prior_context}' || prompt_template
WHERE name = 'implement';
