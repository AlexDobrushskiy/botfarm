-- Migration 034: Add manual-qa pipeline template.
-- Tickets with the "manual-qa" label are routed to this pipeline instead of
-- the default implementation pipeline.  The single "qa" stage performs QA
-- validation against the PR and emits a qa_report result.

INSERT OR IGNORE INTO pipeline_templates (name, description, ticket_label, is_default)
VALUES (
    'manual-qa',
    'QA pipeline: validate a PR against its ticket requirements and report results',
    'manual-qa',
    0
);

INSERT OR IGNORE INTO stage_templates (
    pipeline_id, name, stage_order, executor_type, identity,
    prompt_template, max_turns, timeout_minutes, result_parser
)
VALUES (
    (SELECT id FROM pipeline_templates WHERE name = 'manual-qa'),
    'qa', 1, 'claude', 'coder',
    'You are performing QA on ticket {ticket_id}.

Review the PR and verify that the implementation satisfies the ticket requirements. Check for:
- Correctness: does the code do what the ticket asks?
- Edge cases and error handling
- Test coverage for the changes
- Any regressions or unintended side effects

When finished, output your findings in this JSON format:
```json
{
  "type": "qa_report",
  "passed": true/false,
  "summary": "Brief overall assessment",
  "bugs": [
    {"title": "Bug title", "description": "Details", "severity": "critical|high|medium|low"}
  ],
  "report_text": "Detailed findings"
}
```',
    200, 60, 'qa_report'
);
