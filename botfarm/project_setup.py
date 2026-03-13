"""Shared project setup logic for CLI and dashboard.

Extracted from ``cli.py`` so both the CLI ``add-project`` command and the
dashboard API can reuse the same helpers for cloning repos, creating
worktrees, and updating config.yaml.
"""

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import yaml

from botfarm.config import DEFAULT_CONFIG_DIR, ConfigError


# Known default project names created by ``botfarm init`` templates.
INIT_PLACEHOLDER_NAMES = frozenset({"my-project", "project"})


class ProjectSetupError(Exception):
    """Raised when a project setup step fails."""


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def yaml_scalar(value):
    """Format a Python value as a YAML scalar or flow-style sequence."""
    if isinstance(value, list):
        return "[" + ", ".join(str(x) for x in value) + "]"
    s = str(value)
    if not s:
        return '""'
    try:
        roundtripped = yaml.safe_load(s)
    except yaml.YAMLError:
        roundtripped = None
    if roundtripped != s:
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


def detect_project_indent(raw: str) -> int:
    """Detect the indentation level of project list items in config text.

    Returns the number of leading spaces before ``- name:`` in the first
    project entry found under the top-level ``projects:`` key.  Defaults
    to 2 when no existing entries are found.
    """
    in_projects = False
    for line in raw.split("\n"):
        stripped = line.strip()
        if not in_projects:
            if stripped.startswith("projects:") and line and not line[0].isspace():
                in_projects = True
            continue
        if stripped.startswith("- name:"):
            return len(line) - len(line.lstrip())
    return 2


def format_project_entry(project_dict: dict, indent: int = 2) -> str:
    """Format a project dict as a YAML list item."""
    prefix = " " * indent
    cont = " " * (indent + 2)
    lines = [
        f"{prefix}- name: {yaml_scalar(project_dict['name'])}",
        f"{cont}linear_team: {yaml_scalar(project_dict['linear_team'])}",
        f"{cont}base_dir: {yaml_scalar(project_dict['base_dir'])}",
        f"{cont}worktree_prefix: {yaml_scalar(project_dict['worktree_prefix'])}",
        f"{cont}slots: {yaml_scalar(project_dict['slots'])}",
    ]
    if project_dict.get("linear_project"):
        lines.append(
            f"{cont}linear_project: {yaml_scalar(project_dict['linear_project'])}"
        )
    return "\n".join(lines)


def find_projects_insert_point(lines: list[str]) -> int:
    """Return the line index after the last content line of the projects section."""
    in_projects = False
    last_content_idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip()

        if not in_projects:
            if stripped.startswith("projects:") and not line[0].isspace():
                in_projects = True
                last_content_idx = i
            continue

        # Any non-empty, non-indented line that isn't a list item ends the section
        if stripped and not line[0].isspace() and not stripped.startswith("- "):
            break

        # Count content lines as part of the projects section
        if stripped:
            last_content_idx = i

    return last_content_idx + 1


def write_text_atomic(path: Path, text: str) -> None:
    """Write text to a file atomically via temp file + rename."""
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.replace(tmp, str(path))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def remove_project_entry_text(raw: str, name: str) -> str:
    """Remove a single project list entry by name from config text.

    Preserves all comments and formatting outside the removed entry.
    Returns the raw text unchanged if the entry is not found.
    """
    lines = raw.split("\n")
    entry_start = None
    entry_end = None

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("- name:"):
            continue
        name_part = stripped.split(":", 1)[1]
        # Strip inline YAML comment
        if " #" in name_part:
            name_part = name_part.split(" #")[0]
        entry_name = name_part.strip().strip('"').strip("'")

        if entry_name != name:
            continue

        entry_start = i
        # Scan forward to find the end of this entry
        for j in range(i + 1, len(lines)):
            jstripped = lines[j].strip()
            # Next list item
            if jstripped.startswith("- "):
                entry_end = j
                break
            # Left the indented section (non-empty, non-indented line)
            if jstripped and lines[j] and not lines[j][0].isspace():
                entry_end = j
                break
        if entry_end is None:
            entry_end = len(lines)
        break

    if entry_start is None:
        return raw

    new_lines = lines[:entry_start] + lines[entry_end:]
    return "\n".join(new_lines)


# ---------------------------------------------------------------------------
# Project validation helpers
# ---------------------------------------------------------------------------


def is_placeholder_project(entry: dict) -> bool:
    """Check if a project entry is an unconfigured init placeholder.

    Only matches entries whose name is a known init default (e.g.
    ``my-project``) AND whose ``base_dir`` doesn't exist on disk.
    Projects with non-default names are never flagged, even if their
    directory is temporarily missing (unmounted drive, moved, etc.).
    """
    name = entry.get("name", "")
    if name not in INIT_PLACEHOLDER_NAMES:
        return False
    base_dir = entry.get("base_dir", "")
    if not base_dir:
        return True
    expanded = Path(base_dir).expanduser()
    if expanded.is_dir():
        return False
    # Don't flag projects under the botfarm projects directory — they
    # were likely added via add-project and the repo was later removed.
    botfarm_projects = (DEFAULT_CONFIG_DIR / "projects").expanduser()
    try:
        expanded.relative_to(botfarm_projects)
        return False
    except ValueError:
        return True


def run_readiness_checks(project_dict: dict) -> list[tuple[str, str]]:
    """Run CLAUDE.md and runtime readiness checks for a single project.

    Returns a list of (level, message) tuples where level is 'warning' or 'ok'.
    """
    from botfarm.config import ProjectConfig

    project = ProjectConfig(**project_dict)
    results: list[tuple[str, str]] = []

    # CLAUDE.md check
    base = Path(project.base_dir).expanduser()
    claude_md = base / "CLAUDE.md"
    if claude_md.exists():
        results.append(("ok", f"CLAUDE.md found: {claude_md}"))
    else:
        results.append((
            "warning",
            f"No CLAUDE.md — agent will lack project-specific instructions. "
            f"Create one with: botfarm init-claude-md {base}",
        ))

    # Runtime detection check
    from botfarm.preflight import detect_runtimes

    for level, language, message in detect_runtimes(base):
        if level == "ok":
            results.append(("ok", f"{language} runtime: {message}"))
        else:
            results.append(("warning", message))

    return results


# ---------------------------------------------------------------------------
# Config manipulation
# ---------------------------------------------------------------------------


def append_project_to_config(
    config_path: Path,
    project_dict: dict,
    *,
    replace_names: frozenset[str] = frozenset(),
) -> None:
    """Append a new project entry to the config.yaml projects list.

    Uses text-based insertion instead of full YAML rewrite to preserve
    comments in the config file.  When *replace_names* is given, those
    project entries are removed before the new entry is appended.
    """
    raw = config_path.read_text()

    # Detect indent style before removing entries (removals may empty the list)
    indent = detect_project_indent(raw)

    # Remove placeholder entries first
    for name in replace_names:
        raw = remove_project_entry_text(raw, name)

    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    projects = data.get("projects")
    if projects is not None and not isinstance(projects, list):
        raise ConfigError(
            "Config 'projects' key exists but is not a list — "
            "fix the config file manually before adding a project"
        )

    entry_text = format_project_entry(project_dict, indent=indent)

    if "projects" not in data:
        new_raw = f"projects:\n{entry_text}\n\n{raw}"
    elif not data.get("projects"):
        # Empty list (projects: []) or null — replace the projects line
        lines = raw.split("\n")
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("projects:") and not line[0].isspace():
                lines[i] = f"projects:\n{entry_text}"
                break
        new_raw = "\n".join(lines)
    else:
        lines = raw.split("\n")
        insert_at = find_projects_insert_point(lines)
        lines.insert(insert_at, entry_text)
        new_raw = "\n".join(lines)

    write_text_atomic(config_path, new_raw)


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------


def extract_repo_name(repo_url: str) -> str:
    """Extract a project name from a git repo URL.

    Handles SSH (git@github.com:user/repo.git) and HTTPS
    (https://github.com/user/repo.git) URLs.
    """
    url = repo_url.rstrip("/")
    if url.endswith(".git"):
        url = url[:-4]
    match = re.search(r"[:/]([^/]+)$", url)
    if match:
        return match.group(1)
    return url.split("/")[-1] or "project"


def clone_repo(repo_url: str, target_dir: Path) -> None:
    """Clone a git repository to the target directory.

    Raises ProjectSetupError on failure.
    """
    try:
        result = subprocess.run(
            ["git", "clone", repo_url, str(target_dir)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            raise ProjectSetupError(
                f"git clone failed:\n{result.stderr.strip()}"
            )
    except FileNotFoundError:
        raise ProjectSetupError("git is not installed or not in PATH")
    except subprocess.TimeoutExpired:
        raise ProjectSetupError("git clone timed out after 5 minutes")


def create_worktree(repo_dir: Path, worktree_path: Path, branch_name: str) -> None:
    """Create a git worktree with a new branch.

    Raises ProjectSetupError on failure.
    """
    try:
        result = subprocess.run(
            [
                "git", "-C", str(repo_dir),
                "worktree", "add",
                "-b", branch_name,
                str(worktree_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise ProjectSetupError(
                f"Failed to create worktree at {worktree_path}:\n"
                f"{result.stderr.strip()}"
            )
    except subprocess.TimeoutExpired:
        raise ProjectSetupError(
            f"git worktree add timed out for {worktree_path}"
        )


# ---------------------------------------------------------------------------
# High-level orchestrator
# ---------------------------------------------------------------------------


def setup_project(
    repo_url: str,
    name: str,
    linear_team: str,
    linear_project: str,
    slots: list[int],
    config_path: Path,
    projects_dir: Path | None = None,
    *,
    replace_names: frozenset[str] = frozenset(),
    progress_callback: Callable[[str], None] | None = None,
) -> dict:
    """Set up a new project end-to-end: clone, worktrees, config update.

    This is the main entry point for both CLI and dashboard. It is
    synchronous (designed to run in a thread for non-blocking callers).

    Args:
        repo_url: Git repository URL (SSH or HTTPS).
        name: Project name.
        linear_team: Linear team key (e.g. "SMA").
        linear_project: Linear project filter (empty string for none).
        slots: List of slot IDs (e.g. [1, 2, 3]).
        config_path: Path to config.yaml.
        projects_dir: Directory for repo clone and worktrees.
            Defaults to ``~/.botfarm/projects/<name>``.
        replace_names: Set of project names to remove from config
            before adding the new entry (used for placeholder replacement).
        progress_callback: Optional callback invoked with status messages
            so callers (e.g. dashboard SSE) can stream progress.

    Returns:
        The project dict that was written to config.

    Raises:
        ProjectSetupError: On any failure (cleanup is performed automatically).
    """

    def _progress(msg: str) -> None:
        if progress_callback is not None:
            progress_callback(msg)

    if projects_dir is None:
        projects_dir = DEFAULT_CONFIG_DIR / "projects" / name

    repo_dir = projects_dir / "repo"
    worktree_prefix = f"{name}-slot-"

    if repo_dir.exists():
        raise ProjectSetupError(
            f"Directory already exists: {repo_dir}\n"
            f"Remove it first or choose a different project name."
        )

    # Track whether we created projects_dir so we can clean up on failure
    created_projects_dir = not projects_dir.exists()

    try:
        projects_dir.mkdir(parents=True, exist_ok=True)

        _progress("Cloning repository...")
        clone_repo(repo_url, repo_dir)
        _progress(f"Cloned to {repo_dir}")

        _progress(f"Creating {len(slots)} worktree(s)...")
        for slot_id in slots:
            branch_name = f"slot-{slot_id}-placeholder"
            worktree_path = projects_dir / f"{worktree_prefix}{slot_id}"
            create_worktree(repo_dir, worktree_path, branch_name)
            _progress(f"Slot {slot_id}: {worktree_path} (branch: {branch_name})")

        _progress("Updating config...")
        project_dict = {
            "name": name,
            "linear_team": linear_team,
            "base_dir": str(repo_dir),
            "worktree_prefix": str(projects_dir / worktree_prefix),
            "slots": slots,
            "linear_project": linear_project,
        }

        append_project_to_config(
            config_path, project_dict, replace_names=replace_names,
        )
        _progress(f"Updated {config_path}")

    except (ProjectSetupError, ConfigError, OSError) as exc:
        _progress("Error — cleaning up...")
        if created_projects_dir and projects_dir.exists():
            shutil.rmtree(projects_dir, ignore_errors=True)
            _progress(f"Removed {projects_dir}")
        elif projects_dir.exists():
            if repo_dir.exists():
                shutil.rmtree(repo_dir, ignore_errors=True)
            for slot_id in slots:
                wt = projects_dir / f"{worktree_prefix}{slot_id}"
                if wt.exists():
                    shutil.rmtree(wt, ignore_errors=True)
            _progress(f"Cleaned up cloned repo and worktrees from {projects_dir}")
        if isinstance(exc, ProjectSetupError):
            raise
        raise ProjectSetupError(str(exc)) from exc

    # Readiness checks (informational — don't fail setup)
    readiness = run_readiness_checks(project_dict)
    for level, msg in readiness:
        tag = "OK" if level == "ok" else "WARN"
        _progress(f"  {tag}  {msg}")

    _progress(f"Project '{name}' added successfully!")
    return project_dict
