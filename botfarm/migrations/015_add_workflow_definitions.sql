-- Workflow pipeline definition tables: pipeline_templates, stage_templates, stage_loops.
-- Seed data for the current hardcoded implementation and investigation pipelines.

CREATE TABLE IF NOT EXISTS pipeline_templates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT UNIQUE NOT NULL,
    description TEXT,
    ticket_label TEXT,
    is_default  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS stage_templates (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id      INTEGER NOT NULL REFERENCES pipeline_templates(id),
    name             TEXT NOT NULL,
    stage_order      INTEGER NOT NULL,
    executor_type    TEXT NOT NULL,
    identity         TEXT,
    prompt_template  TEXT,
    max_turns        INTEGER,
    timeout_minutes  INTEGER,
    shell_command    TEXT,
    result_parser    TEXT,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(pipeline_id, name),
    UNIQUE(pipeline_id, stage_order)
);

CREATE TABLE IF NOT EXISTS stage_loops (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id      INTEGER NOT NULL REFERENCES pipeline_templates(id),
    name             TEXT NOT NULL,
    start_stage      TEXT NOT NULL,
    end_stage        TEXT NOT NULL,
    max_iterations   INTEGER NOT NULL,
    config_key       TEXT,
    exit_condition   TEXT,
    on_failure_stage TEXT,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(pipeline_id, name)
);

-- ---------------------------------------------------------------------------
-- Seed: Implementation pipeline
-- ---------------------------------------------------------------------------

INSERT INTO pipeline_templates (name, description, ticket_label, is_default)
VALUES (
    'implementation',
    'Standard implementation pipeline: implement, review, fix, CI checks, merge',
    NULL,
    1
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    1, 'implement', 1, 'claude', 'coder',
    'Work on Linear ticket {ticket_id}. Follow the Linear Tickets workflow in CLAUDE.md. Complete all steps through PR creation. Do not stop until the PR is created.',
    200, 120, 'pr_url'
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    1, 'review', 2, 'claude', 'reviewer',
    'Review the pull request at {pr_url}. Read the PR diff carefully. Be thorough but constructive.

For file-specific feedback, post inline review comments on the exact lines where changes are needed. First get the head SHA with:
  gh pr view {pr_number} --json headRefOid --jq .headRefOid
Then post each inline comment using:
  gh api repos/{owner}/{repo}/pulls/{pr_number}/comments -f body=''comment'' -f commit_id=''HEAD_SHA'' -f path=''file.py'' -F line=42 -f side=''RIGHT''

After posting all inline comments, submit your overall assessment using ''gh pr review'' with either --approve or --request-changes and a summary body.

IMPORTANT: If you posted ANY inline comments with suggestions, issues, or actionable feedback, you MUST use --request-changes and output VERDICT: CHANGES_REQUESTED. Only use --approve and VERDICT: APPROVED when there are ZERO actionable inline comments.

At the very end of your response, output exactly one of these verdict markers on its own line:
  VERDICT: APPROVED
  VERDICT: CHANGES_REQUESTED',
    100, 30, 'review_verdict'
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    1, 'fix', 3, 'claude', 'coder',
    'Address the review comments on PR {pr_url}. Read both the top-level review comment and any inline review comments on specific files/lines. Use ''gh api repos/{owner}/{repo}/pulls/{pr_number}/comments'' to list inline comments. Note each comment''s `id` field from the API response.

Make the necessary code changes for each comment, run tests, commit and push the fixes.

After addressing (or deciding to skip) each inline comment, reply to it using:
  gh api repos/{owner}/{repo}/pulls/{pr_number}/comments/COMMENT_ID/replies -f body=''...''
Replace COMMENT_ID with the comment''s `id` from the earlier API response.

Reply guidelines:
- If fixed and obvious from context: reply "Fixed"
- If fixed but clarification helps: reply "Fixed — [what changed]"
- If intentionally not fixed: reply with a brief explanation why',
    100, 60, NULL
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, shell_command, result_parser)
VALUES (
    1, 'pr_checks', 4, 'shell', NULL,
    NULL,
    NULL, NULL,
    'gh pr checks {pr_url} --watch',
    NULL
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    1, 'ci_fix', 5, 'claude', 'coder',
    'The CI checks on PR {pr_url} have failed. Diagnose and fix the CI failures based on the output below, then run tests locally, commit and push the fixes.

CI failure output:
{ci_failure_output}',
    100, 60, NULL
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    1, 'merge', 6, 'internal', NULL,
    NULL,
    NULL, NULL, NULL
);

-- Implementation pipeline loops
INSERT INTO stage_loops (pipeline_id, name, start_stage, end_stage, max_iterations, config_key, exit_condition, on_failure_stage)
VALUES (
    1, 'review_loop', 'review', 'fix', 3, 'max_review_iterations', 'review_approved', NULL
);

INSERT INTO stage_loops (pipeline_id, name, start_stage, end_stage, max_iterations, config_key, exit_condition, on_failure_stage)
VALUES (
    1, 'ci_retry_loop', 'ci_fix', 'pr_checks', 2, 'max_ci_retries', 'ci_passed', 'ci_fix'
);

-- ---------------------------------------------------------------------------
-- Seed: Investigation pipeline
-- ---------------------------------------------------------------------------

INSERT INTO pipeline_templates (name, description, ticket_label, is_default)
VALUES (
    'investigation',
    'Investigation pipeline: research and report findings, no PR created',
    'Investigation',
    0
);

INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    2, 'implement', 1, 'claude', 'coder',
    'Work on Linear ticket {ticket_id}. This is an investigation ticket. Produce a summary of findings as a Linear comment on the ticket. If you identify implementation work, create follow-up Linear tickets. Do not create a PR.',
    200, 30, NULL
);
