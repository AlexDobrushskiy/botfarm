-- Add {shared_mem_path} references to implement, fix, and ci_fix stage templates
-- so the default DB-backed pipeline actually uses shared memory.

UPDATE stage_templates
SET prompt_template = prompt_template || '

After completing your implementation, write a brief summary to {shared_mem_path}/implementer.md containing:
- Files created or modified (with brief description of changes)
- Key architectural decisions made
- Areas of the codebase that are relevant to the change
- Any gotchas or non-obvious implementation details'
WHERE pipeline_id = 1 AND name = 'implement';

UPDATE stage_templates
SET prompt_template = prompt_template || '

Before starting, read {shared_mem_path}/implementer.md (if it exists) for context from the implementer about what was changed and why.'
WHERE pipeline_id = 1 AND name = 'fix';

UPDATE stage_templates
SET prompt_template = prompt_template || '

Before starting, read {shared_mem_path}/implementer.md (if it exists) for context from the implementer about what was changed and why.'
WHERE pipeline_id = 1 AND name = 'ci_fix';
