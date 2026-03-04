-- Migration 021: Add worktree anchoring to stage prompt templates.
--
-- Claude agents discover the base repository via git internals (e.g.
-- git rev-parse --git-common-dir) and switch all git operations there.
-- Appending a {worktree_path} instruction to each prompt template anchors
-- the agent to its assigned worktree directory.

-- Implementation pipeline: implement stage
UPDATE stage_templates
SET prompt_template = prompt_template || '

IMPORTANT: Your working directory is {worktree_path} — this is a git worktree. Never cd to any other directory for git operations. All git, test, and build commands must run in {worktree_path}. Do NOT use the base repository directory.'
WHERE name = 'implement' AND pipeline_id = 1 AND executor_type = 'claude';

-- Implementation pipeline: review stage
UPDATE stage_templates
SET prompt_template = prompt_template || '

IMPORTANT: Your working directory is {worktree_path} — this is a git worktree. Never cd to any other directory for git operations. All git, test, and build commands must run in {worktree_path}. Do NOT use the base repository directory.'
WHERE name = 'review' AND pipeline_id = 1 AND executor_type = 'claude';

-- Implementation pipeline: fix stage
UPDATE stage_templates
SET prompt_template = prompt_template || '

IMPORTANT: Your working directory is {worktree_path} — this is a git worktree. Never cd to any other directory for git operations. All git, test, and build commands must run in {worktree_path}. Do NOT use the base repository directory.'
WHERE name = 'fix' AND pipeline_id = 1 AND executor_type = 'claude';

-- Implementation pipeline: ci_fix stage
UPDATE stage_templates
SET prompt_template = prompt_template || '

IMPORTANT: Your working directory is {worktree_path} — this is a git worktree. Never cd to any other directory for git operations. All git, test, and build commands must run in {worktree_path}. Do NOT use the base repository directory.'
WHERE name = 'ci_fix' AND pipeline_id = 1 AND executor_type = 'claude';

-- Investigation pipeline: implement stage
UPDATE stage_templates
SET prompt_template = prompt_template || '

IMPORTANT: Your working directory is {worktree_path} — this is a git worktree. Never cd to any other directory for git operations. All git, test, and build commands must run in {worktree_path}. Do NOT use the base repository directory.'
WHERE name = 'implement' AND pipeline_id = 2 AND executor_type = 'claude';
