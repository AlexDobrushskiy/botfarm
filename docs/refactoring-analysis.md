# Refactoring Analysis Procedure

This document defines the procedure an agent follows when it picks up a refactoring analysis Investigation ticket.

## 1. Analysis Procedure

### Step 1: Scope the Analysis (5 min)

- Read the current `docs/refactoring-analysis.md` for any evolving thresholds
- Check the previous refactoring analysis ticket (if any) for context on what was flagged last time
- Note any recent large PRs or new features since last analysis (via `git log --since`)

### Step 2: Quantitative Assessment

Measure for each `.py` file under `botfarm/`:

- Lines of code (via `wc -l`)
- Number of functions/classes (via grep for `def ` / `class `)
- For classes with many methods, count methods (`def ` inside the class) and instance variables (`self.` assignments in `__init__`)
- Identify any files that have grown significantly since the last analysis

Focus on files that exceed these **action thresholds** (not aspirational targets — these are "something is wrong" levels):

- Single file > 2,500 lines
- Single function > 200 lines
- Single class > 50 methods or > 30 instance variables
- 3+ near-identical code blocks (obvious copy-paste)

### Step 3: Qualitative Assessment

For files that exceeded quantitative thresholds, read them and evaluate:

- Is the complexity justified? (e.g., a 300-line function that's genuinely sequential and clear may be fine)
- Are there natural seams where extraction would meaningfully improve readability?
- Would an agent working on this file next month struggle to understand and modify it?
- Is there code duplication that's causing real maintenance issues (bugs fixed in one copy but not another)?

### Step 4: Make the Decision

Apply this decision framework:

```
IF no files exceed action thresholds:
    → Verdict: "Good enough" — no refactoring needed

ELSE IF files exceed thresholds but complexity is justified and readable:
    → Verdict: "Good enough" — note the files for monitoring, but no action

ELSE IF files exceed thresholds AND complexity is unjustified:
    → Verdict: "Refactoring needed" — create tickets
```

**Key principle: When in doubt, choose "good enough."** We run this analysis regularly, so missing something this round means catching it next round. False positives (unnecessary refactoring) are more costly than false negatives (delayed refactoring).

## 2. "Good Enough" Outcome

When no refactoring is needed:

1. Post a comment on the Investigation ticket:

   ```
   ## Refactoring Analysis — {month} {year}

   **Verdict: No refactoring needed.**

   Summary:
   - Largest file: {file} ({lines} lines) — within acceptable range
   - {N} files checked, {M} exceeded action thresholds but complexity is justified
   - No significant duplication or structural concerns identified
   - Notable growth since last analysis: {file} grew by {N} lines (still manageable)

   Next scheduled analysis: ~{date based on cadence}
   ```

2. End with a final summary that includes the phrase **"No refactoring needed"** or **"No action needed"** so the supervisor can parse it and send the appropriate notification automatically. Status transitions are handled by the supervisor — do not move the ticket manually.

## 3. "Refactoring Needed" Outcome

When refactoring is warranted:

1. Create a parent ticket: "Codebase Refactoring — {month} {year}" with:
   - Summary of findings (which files, what problems, quantitative evidence)
   - Recommended execution order
   - Risk assessment

2. Create child implementation tickets for each refactoring area:
   - Clear "What to Do" section with specific extraction/decomposition instructions
   - Constraints (tests must pass, no behavioral changes, etc.)
   - Set dependencies between tickets where appropriate

3. Post a comment on the Investigation ticket with a findings summary and link to the parent ticket:

   ```
   ## Refactoring Analysis — {month} {year}

   **Verdict: Refactoring needed.**

   Findings:
   - {file}: {problem} ({quantitative evidence})
   - {file}: {problem} ({quantitative evidence})

   Created {N} refactoring tickets under {parent-id}:
   - {child-id}: {title}
   - {child-id}: {title}
   ```
4. End with a final summary that includes the phrase **"Created {N} refactoring tickets under {parent-id}"** (e.g., "Created 3 refactoring tickets under SMA-456. Top concerns: supervisor.py complexity, duplicate config parsing"). The supervisor parses this to send the appropriate notification automatically. Status transitions are handled by the supervisor — do not move the ticket manually.

## 4. What NOT to Flag

Explicit list of things that should NOT trigger refactoring tickets:

- Missing docstrings or type annotations (unless the agent specifically can't understand the code)
- Style preferences (naming conventions, import ordering)
- "Nice to have" abstractions that aren't solving a real problem
- Files that are large but well-organized and readable
- Test files (test duplication is acceptable if tests are clear)
- Code that was recently written and may still be evolving

## 5. Threshold Evolution

The action thresholds in this document can be adjusted based on experience. If the analysis consistently finds "good enough" for 3+ consecutive runs, consider tightening thresholds slightly. If it consistently flags things that don't warrant action, loosen them.
