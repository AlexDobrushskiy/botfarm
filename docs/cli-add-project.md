# CLI: add-project Command

Detailed reference for the `botfarm add-project` command implementation. Useful for building alternative frontends (dashboard) or understanding the setup pipeline.

**Source:** `botfarm/cli.py` (lines ~1000–1535)

## Overview

Interactive CLI command that guides users through configuring a new project: collecting inputs, cloning the repo, creating git worktrees, writing config, and running readiness checks.

## Input Collection

| Field | Prompt | Validation | Auto-suggest |
|-------|--------|------------|--------------|
| Repository URL | Text input | Must be valid git URL (SSH or HTTPS) | — |
| Project name | Text input | No duplicates with existing projects | Extracted from repo URL via `_extract_repo_name()` |
| Linear team | Selection | Must exist in Linear | Fetched from Linear API if `LINEAR_API_KEY` set, otherwise manual |
| Linear project | Selection (optional) | — | Fetched from Linear API filtered by team |
| Number of slots | Integer (1–20) | Range check | Default: 1 |

Placeholder projects (from `botfarm init`) are detected via `_is_placeholder_project()` and offered for replacement.

## Setup Pipeline

After input collection and user confirmation, the command executes these steps in order:

1. **Create project directory** — `~/.botfarm/projects/<name>/`
2. **Clone repository** — `git clone <url> ~/.botfarm/projects/<name>/repo`
3. **Create worktrees** — one per slot: `git worktree add --detach <worktree_path> origin/main`, then `git checkout -b slot-<N>-placeholder`
4. **Write config** — append project entry to `config.yaml` (comment-preserving)
5. **Run readiness checks** — validate CLAUDE.md present, runtimes installed

On failure at any step, cleanup removes the cloned repo and any created worktrees.

## Helper Functions

All currently private to `cli.py` (prefixed with `_`). Candidates for extraction to a shared module.

### Config Writing

| Function | Purpose |
|----------|---------|
| `_append_project_to_config(config_path, project_dict, replace_names)` | Main entry point — appends a project entry to YAML, optionally replacing placeholder entries |
| `_format_project_entry(project, indent)` | Formats a project dict as a YAML list item with proper indentation |
| `_yaml_scalar(value)` | Properly quotes YAML scalars (handles special chars) |
| `_detect_project_indent(text)` | Detects existing YAML indentation style (0, 2, 4 spaces) |
| `_find_projects_insert_point(lines)` | Finds the line index to insert a new project entry (after last existing entry) |
| `_remove_project_entry_text(text, name)` | Surgically removes a named project entry from YAML text |
| `_write_text_atomic(path, text)` | Atomic write via temp file + `os.replace()` — prevents corruption |

**Key design:** Config writing uses text-based insertion (not full YAML rewrite) to preserve all comments and formatting. The `_detect_project_indent()` function ensures new entries match existing style.

### Git Operations

| Function | Purpose |
|----------|---------|
| `_clone_repo(repo_url, target_dir)` | Runs `git clone` subprocess |
| `_create_worktree(repo_dir, worktree_path, branch_name)` | Runs `git worktree add --detach` + `git checkout -b <branch>` |
| `_extract_repo_name(repo_url)` | Parses project name from SSH/HTTPS git URL (handles `.git` suffix, org prefixes) |

### Validation

| Function | Purpose |
|----------|---------|
| `_is_placeholder_project(entry, projects_dir)` | Detects unconfigured init placeholder projects |
| `_run_readiness_checks(project_dict)` | Returns list of `(severity, message)` tuples. Checks: CLAUDE.md exists, runtime dependencies present |

## Config Entry Format

The project dict written to config:

```yaml
- name: my-project
  linear_team: TEAM
  base_dir: ~/.botfarm/projects/my-project/repo
  worktree_prefix: ~/.botfarm/projects/my-project/slot-
  slots: [1, 2, 3]
  linear_project: "Optional Filter"    # only included if non-empty
```

## Validation Rules (from config.py)

- `name` — required, must be unique across all projects
- `linear_team` — required
- `base_dir` — required
- `worktree_prefix` — required
- `slots` — required, list of integers, no duplicates
- `linear_project` — optional, must be unique across projects when set (prevents ticket routing ambiguity)

## Error Handling

- Duplicate project name → `click.ClickException` (before any filesystem work)
- Git clone failure → cleanup cloned dir, raise `click.ClickException`
- Worktree creation failure → cleanup all worktrees + cloned dir
- Config write failure → `ConfigError` (filesystem already set up — manual cleanup may be needed)
- Readiness check failures → warnings displayed but don't block completion
