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
        f"{cont}team: {yaml_scalar(project_dict['team'])}",
        f"{cont}base_dir: {yaml_scalar(project_dict['base_dir'])}",
        f"{cont}worktree_prefix: {yaml_scalar(project_dict['worktree_prefix'])}",
        f"{cont}slots: {yaml_scalar(project_dict['slots'])}",
    ]
    if project_dict.get("tracker_project"):
        lines.append(
            f"{cont}tracker_project: {yaml_scalar(project_dict['tracker_project'])}"
        )
    if project_dict.get("project_type"):
        lines.append(
            f"{cont}project_type: {yaml_scalar(project_dict['project_type'])}"
        )
    if project_dict.get("setup_commands"):
        lines.append(f"{cont}setup_commands:")
        for cmd in project_dict["setup_commands"]:
            lines.append(f"{cont}  - {yaml_scalar(cmd)}")
    if project_dict.get("run_command"):
        lines.append(
            f"{cont}run_command: {yaml_scalar(project_dict['run_command'])}"
        )
    if project_dict.get("run_port"):
        lines.append(f"{cont}run_port: {project_dict['run_port']}")
    if project_dict.get("run_env"):
        lines.append(f"{cont}run_env:")
        for k, v in project_dict["run_env"].items():
            lines.append(f"{cont}  {yaml_scalar(k)}: {yaml_scalar(v)}")
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


# GIT_* environment variables that override which repository git operates on.
# These must be stripped when spawning git subprocesses that target a different
# repo (e.g. a newly-inited project) so the parent context (such as a
# pre-commit hook's GIT_DIR) doesn't leak through.
_GIT_REPO_ENV_VARS = frozenset({
    "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
})


def _clean_git_env() -> dict[str, str]:
    """Return a copy of os.environ without repo-binding ``GIT_*`` vars.

    Preserves identity vars (``GIT_AUTHOR_*``, ``GIT_COMMITTER_*``) so
    that commits still work when the global git config lacks ``user.*``.
    """
    return {k: v for k, v in os.environ.items()
            if k not in _GIT_REPO_ENV_VARS}


def is_git_repo(path: Path) -> bool:
    """Check if a directory is the root of a git repository (or worktree).

    Uses a filesystem check (presence of ``.git`` dir or file) rather than
    spawning ``git`` so that the result is unaffected by inherited ``GIT_*``
    environment variables (e.g. inside a pre-commit hook).
    """
    git_indicator = path / ".git"
    # .git is a directory in a normal repo, or a file in a worktree
    return git_indicator.is_dir() or git_indicator.is_file()


_GITIGNORE_TEMPLATE = """\
# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
.venv/
venv/

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.local
"""

_README_TEMPLATE = """\
# {name}

Project managed by [botfarm](https://github.com/AlexDobrushskiy/botfarm).
"""

_CLAUDE_MD_TEMPLATE = """\
# CLAUDE.md

## Project Context
<!-- Describe what this project does and its architecture -->

## Development
<!-- Commands to build, test, and run the project -->

## Guidelines
<!-- Coding conventions, patterns, and things to watch out for -->
"""


def init_repo(target_dir: Path, name: str) -> None:
    """Initialize a new git repository with basic project structure.

    Creates the directory, runs ``git init``, writes .gitignore, README.md,
    and CLAUDE.md template, then makes an initial commit.

    Raises ProjectSetupError on failure.
    """
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ProjectSetupError(f"Cannot create directory {target_dir}: {exc}")

    env = _clean_git_env()
    try:
        result = subprocess.run(
            ["git", "init", str(target_dir)],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        if result.returncode != 0:
            raise ProjectSetupError(
                f"git init failed:\n{result.stderr.strip()}"
            )
    except FileNotFoundError:
        raise ProjectSetupError("git is not installed or not in PATH")
    except subprocess.TimeoutExpired:
        raise ProjectSetupError("git init timed out")

    # Write starter files
    (target_dir / ".gitignore").write_text(_GITIGNORE_TEMPLATE)
    (target_dir / "README.md").write_text(_README_TEMPLATE.format(name=name))
    (target_dir / "CLAUDE.md").write_text(_CLAUDE_MD_TEMPLATE)

    # Set repo-local identity so the initial commit works even without
    # global user.name/user.email configuration.
    subprocess.run(
        ["git", "-C", str(target_dir), "config", "user.name", "botfarm"],
        capture_output=True, text=True, timeout=10, env=env,
    )
    subprocess.run(
        ["git", "-C", str(target_dir), "config", "user.email", "botfarm@localhost"],
        capture_output=True, text=True, timeout=10, env=env,
    )

    # Initial commit
    result = subprocess.run(
        ["git", "-C", str(target_dir), "add", "."],
        capture_output=True, text=True, timeout=10, env=env,
    )
    if result.returncode != 0:
        raise ProjectSetupError(
            f"git add failed:\n{result.stderr.strip()}"
        )
    result = subprocess.run(
        ["git", "-C", str(target_dir), "commit", "-m", "Initial commit"],
        capture_output=True, text=True, timeout=10, env=env,
    )
    if result.returncode != 0:
        raise ProjectSetupError(
            f"Initial commit failed:\n{result.stderr.strip()}"
        )


def create_github_repo(repo_dir: Path, name: str, *, private: bool = True) -> str:
    """Create a GitHub repository and push the local repo to it.

    Uses the ``gh`` CLI.  Returns the URL of the created repository.

    Raises ProjectSetupError on failure.
    """
    visibility = "--private" if private else "--public"
    try:
        result = subprocess.run(
            [
                "gh", "repo", "create", name,
                visibility, "--source", str(repo_dir), "--push",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=_clean_git_env(),
        )
        if result.returncode != 0:
            raise ProjectSetupError(
                f"gh repo create failed:\n{result.stderr.strip()}"
            )
        # gh prints the repo URL on stdout
        return result.stdout.strip()
    except FileNotFoundError:
        raise ProjectSetupError(
            "GitHub CLI (gh) is not installed or not in PATH"
        )
    except subprocess.TimeoutExpired:
        raise ProjectSetupError("gh repo create timed out after 60 seconds")


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
            env=_clean_git_env(),
        )
        if result.returncode != 0:
            raise ProjectSetupError(
                f"git clone failed:\n{result.stderr.strip()}"
            )
    except FileNotFoundError:
        raise ProjectSetupError("git is not installed or not in PATH")
    except subprocess.TimeoutExpired:
        raise ProjectSetupError("git clone timed out after 5 minutes")


def _branch_exists(repo_dir: Path, branch_name: str) -> bool:
    """Check whether a local branch exists in the repository."""
    result = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "--verify", f"refs/heads/{branch_name}"],
        capture_output=True, text=True, timeout=10, env=_clean_git_env(),
    )
    return result.returncode == 0


def create_worktree(
    repo_dir: Path,
    worktree_path: Path,
    branch_name: str,
    *,
    create_branch: bool = True,
) -> None:
    """Create a git worktree, optionally creating a new branch.

    When *create_branch* is ``True`` (default), uses ``git worktree add -b``
    to create a new branch.  When ``False``, attaches the worktree to an
    existing branch via ``git worktree add <path> <branch>``.

    Raises ProjectSetupError on failure.
    """
    cmd = ["git", "-C", str(repo_dir), "worktree", "add"]
    if create_branch:
        cmd += ["-b", branch_name, str(worktree_path)]
    else:
        cmd += [str(worktree_path), branch_name]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env=_clean_git_env(),
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


def _run_setup_commands(
    commands: list[str],
    worktree_paths: list[Path],
    progress: Callable[[str], None],
) -> None:
    """Run setup commands in each worktree directory.

    Failures are reported via the *progress* callback but do not abort
    the setup — the project is still usable and commands can be retried
    via the dashboard's "Setup Git" button.
    """
    for wt_path in worktree_paths:
        slot_name = wt_path.name
        for cmd in commands:
            progress(f"  [{slot_name}] $ {cmd}")
            try:
                result = subprocess.run(
                    cmd,
                    shell=True,
                    cwd=str(wt_path),
                    capture_output=True,
                    text=True,
                    timeout=300,
                    env=_clean_git_env(),
                )
                if result.returncode != 0:
                    stderr = result.stderr.strip()
                    progress(
                        f"  [{slot_name}] WARN: command exited {result.returncode}"
                        + (f": {stderr}" if stderr else "")
                    )
                else:
                    progress(f"  [{slot_name}] OK")
            except subprocess.TimeoutExpired:
                progress(f"  [{slot_name}] WARN: command timed out after 5m")
            except OSError as exc:
                progress(f"  [{slot_name}] WARN: {exc}")


# ---------------------------------------------------------------------------
# High-level orchestrator
# ---------------------------------------------------------------------------


def setup_project(
    repo_url: str,
    name: str,
    team: str,
    tracker_project: str,
    slots: list[int],
    config_path: Path,
    projects_dir: Path | None = None,
    *,
    create_github: bool = False,
    replace_names: frozenset[str] = frozenset(),
    progress_callback: Callable[[str], None] | None = None,
    project_type: str = "",
    setup_commands: list[str] | None = None,
) -> dict:
    """Set up a new project end-to-end: clone or init, worktrees, config update.

    This is the main entry point for both CLI and dashboard. It is
    synchronous (designed to run in a thread for non-blocking callers).

    When *repo_url* is provided the repo is cloned.  When empty, the
    behaviour depends on whether the target directory already exists:

    * Existing git repo → reuse as-is.
    * Non-existent directory → ``git init`` with starter files.

    When *create_github* is ``True`` (and no *repo_url*), a GitHub
    repository is created via the ``gh`` CLI and the local repo is pushed.

    Args:
        repo_url: Git repository URL (SSH or HTTPS).  Empty string to
            initialise a new repo instead of cloning.
        name: Project name.
        team: Bugtracker team key (e.g. "SMA").
        tracker_project: Bugtracker project filter (empty string for none).
        slots: List of slot IDs (e.g. [1, 2, 3]).
        config_path: Path to config.yaml.
        projects_dir: Directory for repo clone and worktrees.
            Defaults to ``~/.botfarm/projects/<name>``.
        create_github: When True and *repo_url* is empty, create a GitHub
            repo via ``gh repo create`` and push.
        replace_names: Set of project names to remove from config
            before adding the new entry (used for placeholder replacement).
        progress_callback: Optional callback invoked with status messages
            so callers (e.g. dashboard SSE) can stream progress.
        project_type: Optional project type (e.g. "python", "node").
        setup_commands: Optional list of shell commands to run in each
            worktree after creation.

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

    # Validate parent directory is writable
    parent = projects_dir.parent
    if parent.exists() and not os.access(str(parent), os.W_OK):
        raise ProjectSetupError(
            f"Parent directory is not writable: {parent}"
        )

    # Determine which repo setup path to take
    reuse_existing = False
    if repo_url:
        # Clone path — repo_dir must not exist
        if repo_dir.exists():
            raise ProjectSetupError(
                f"Directory already exists: {repo_dir}\n"
                f"Remove it first or choose a different project name."
            )
    elif repo_dir.exists() and is_git_repo(repo_dir):
        # Existing git repo at the expected path — reuse it
        reuse_existing = True
        _progress(f"Using existing git repo at {repo_dir}")
    elif repo_dir.exists():
        raise ProjectSetupError(
            f"Directory exists but is not a git repo: {repo_dir}\n"
            f"Remove it or run 'git init' manually."
        )
    # else: repo_dir doesn't exist → will init below

    # Track whether we created projects_dir so we can clean up on failure
    created_projects_dir = not projects_dir.exists()
    # Track worktrees created during this call so we can clean them up
    # on failure even when reusing an existing repo.
    created_worktrees: list[Path] = []

    try:
        projects_dir.mkdir(parents=True, exist_ok=True)

        if repo_url:
            _progress("Cloning repository...")
            clone_repo(repo_url, repo_dir)
            _progress(f"Cloned to {repo_dir}")
        elif not reuse_existing:
            _progress("Initializing new git repository...")
            init_repo(repo_dir, name)
            _progress(f"Initialized {repo_dir}")

        if create_github and not repo_url:
            _progress("Creating GitHub repository...")
            gh_url = create_github_repo(repo_dir, name)
            _progress(f"GitHub repo created: {gh_url}")

        _progress(f"Creating {len(slots)} worktree(s)...")
        for slot_id in slots:
            branch_name = f"slot-{slot_id}-placeholder"
            worktree_path = projects_dir / f"{worktree_prefix}{slot_id}"
            create_worktree(repo_dir, worktree_path, branch_name)
            created_worktrees.append(worktree_path)
            _progress(f"Slot {slot_id}: {worktree_path} (branch: {branch_name})")

        # Run setup commands in each worktree
        if setup_commands:
            _progress(f"Running setup commands in {len(created_worktrees)} worktree(s)...")
            _run_setup_commands(
                setup_commands, created_worktrees, _progress,
            )

        _progress("Updating config...")
        project_dict: dict = {
            "name": name,
            "team": team,
            "base_dir": str(repo_dir),
            "worktree_prefix": str(projects_dir / worktree_prefix),
            "slots": slots,
            "tracker_project": tracker_project,
        }
        if project_type:
            project_dict["project_type"] = project_type
        if setup_commands:
            project_dict["setup_commands"] = setup_commands

        append_project_to_config(
            config_path, project_dict, replace_names=replace_names,
        )
        _progress(f"Updated {config_path}")

    except (ProjectSetupError, ConfigError, OSError) as exc:
        _progress("Error — cleaning up...")
        if created_projects_dir and projects_dir.exists():
            shutil.rmtree(projects_dir, ignore_errors=True)
            _progress(f"Removed {projects_dir}")
        elif projects_dir.exists() and not reuse_existing:
            if repo_dir.exists():
                shutil.rmtree(repo_dir, ignore_errors=True)
            for wt in created_worktrees:
                if wt.exists():
                    shutil.rmtree(wt, ignore_errors=True)
            _progress(f"Cleaned up cloned repo and worktrees from {projects_dir}")
        elif reuse_existing and created_worktrees:
            for wt in created_worktrees:
                if wt.exists():
                    shutil.rmtree(wt, ignore_errors=True)
            _progress(f"Cleaned up newly-created worktrees from {projects_dir}")
        if isinstance(exc, ProjectSetupError):
            raise
        raise ProjectSetupError(str(exc)) from exc

    # Readiness checks (informational — don't fail setup)
    try:
        readiness = run_readiness_checks(project_dict)
    except Exception:
        readiness = []
    for level, msg in readiness:
        tag = "OK" if level == "ok" else "WARN"
        _progress(f"  {tag}  {msg}")

    _progress(f"Project '{name}' added successfully!")
    return project_dict


def setup_project_git(
    name: str,
    config_path: Path,
    *,
    create_github: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> None:
    """Set up or repair git repo and worktrees for an existing project in config.

    Reads the project entry from config, then ensures the repo directory and
    all worktrees exist.  Useful as a retry mechanism when the initial setup
    failed partway through or when worktrees need to be recreated.

    Raises ProjectSetupError on failure.
    """

    def _progress(msg: str) -> None:
        if progress_callback is not None:
            progress_callback(msg)

    raw = config_path.read_text()
    data = yaml.safe_load(raw)
    projects = data.get("projects") or []
    project_entry = None
    for p in projects:
        if p.get("name") == name:
            project_entry = p
            break
    if project_entry is None:
        raise ProjectSetupError(f"Project '{name}' not found in config")

    repo_dir = Path(project_entry["base_dir"]).expanduser()
    wt_prefix = project_entry.get("worktree_prefix", "")
    slot_ids = project_entry.get("slots", [])

    # Validate parent is writable
    parent = repo_dir.parent
    if parent.exists() and not os.access(str(parent), os.W_OK):
        raise ProjectSetupError(f"Parent directory is not writable: {parent}")

    # Repo setup — only repair, never create from scratch.  If the repo
    # dir is missing the project may have been cloned from a remote and
    # re-initing would silently replace it with an empty repo.
    if repo_dir.exists() and is_git_repo(repo_dir):
        _progress(f"Git repo already exists at {repo_dir}")
    elif repo_dir.exists():
        raise ProjectSetupError(
            f"Directory exists but is not a git repo: {repo_dir}"
        )
    else:
        raise ProjectSetupError(
            f"Repository directory does not exist: {repo_dir}\n"
            f"Use the full project creation flow to set up a new project."
        )

    if create_github:
        _progress("Creating GitHub repository...")
        gh_url = create_github_repo(repo_dir, name)
        _progress(f"GitHub repo created: {gh_url}")

    # Prune stale worktree bookkeeping left by interrupted setups so that
    # branches whose directories were removed can be reattached cleanly.
    subprocess.run(
        ["git", "-C", str(repo_dir), "worktree", "prune"],
        capture_output=True, text=True, timeout=30, env=_clean_git_env(),
    )

    # Worktree setup — skip slots that already have healthy worktrees
    all_worktree_paths: list[Path] = []
    for slot_id in slot_ids:
        branch_name = f"slot-{slot_id}-placeholder"
        worktree_path = Path(f"{wt_prefix}{slot_id}").expanduser()
        if worktree_path.exists() and is_git_repo(worktree_path):
            _progress(f"Slot {slot_id}: worktree already exists at {worktree_path}")
            all_worktree_paths.append(worktree_path)
            continue
        if worktree_path.exists():
            # Plain directory left behind by a partial setup — remove so
            # git worktree add can succeed.
            shutil.rmtree(worktree_path, ignore_errors=True)
        _progress(f"Creating worktree for slot {slot_id}...")
        # If the branch already exists (from a prior partial setup), attach
        # to it instead of trying to create a new one with -b.
        branch_already_exists = _branch_exists(repo_dir, branch_name)
        create_worktree(
            repo_dir, worktree_path, branch_name,
            create_branch=not branch_already_exists,
        )
        all_worktree_paths.append(worktree_path)
        _progress(f"Slot {slot_id}: {worktree_path} (branch: {branch_name})")

    # Run setup commands in all worktrees (including existing ones, so
    # that retrying via "Setup Git" can recover from earlier failures).
    setup_commands = project_entry.get("setup_commands") or []
    if setup_commands and all_worktree_paths:
        _progress(f"Running setup commands in {len(all_worktree_paths)} worktree(s)...")
        _run_setup_commands(setup_commands, all_worktree_paths, _progress)

    _progress(f"Git setup for '{name}' complete!")
