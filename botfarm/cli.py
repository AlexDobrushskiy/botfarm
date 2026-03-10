import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

import yaml

from botfarm import __version__

from botfarm.config import (
    DEFAULT_CONFIG_DIR,
    DEFAULT_CONFIG_PATH,
    ConfigError,
    create_default_config,
    load_config,
    write_yaml_atomic,
)
from botfarm.db import (
    SchemaVersionError,
    clear_slot_stage,
    get_codex_review_stats,
    get_stage_run_aggregates,
    get_task_by_ticket,
    get_task_history,
    get_ticket_history_entry,
    init_db,
    insert_event,
    load_all_project_pause_states,
    load_all_slots,
    load_dispatch_state,
    resolve_db_path,
    save_dispatch_state,
    save_project_pause_state,
    update_task,
    upsert_slot,
    upsert_ticket_history,
)
from botfarm.linear import LinearAPIError, LinearClient
from botfarm.linear_cleanup import CleanupService, CooldownError
from botfarm.slots import SlotState, _is_pid_alive
from botfarm.worker import build_git_env, is_protected_branch
from botfarm.systemd_service import UNIT_PATH, generate_unit, install_service, uninstall_service
from botfarm.usage import refresh_usage_snapshot

ENV_FILE_PATH = DEFAULT_CONFIG_DIR / ".env"


def _ensure_local_bin_in_path() -> None:
    """Prepend ``~/.local/bin`` to ``PATH`` if not already present.

    Non-login shells (``nohup``, systemd) don't source ``.bashrc``, so
    ``~/.local/bin`` — the default install location for Claude Code —
    may be missing from PATH.  Called early in ``botfarm run`` so that
    all child processes (workers, Claude, Codex, gh) inherit the fix.
    """
    local_bin = str(Path.home() / ".local" / "bin")
    current_path = os.environ.get("PATH", "")
    if local_bin not in current_path.split(os.pathsep):
        if current_path:
            os.environ["PATH"] = local_bin + os.pathsep + current_path
        else:
            os.environ["PATH"] = local_bin


def _resolve_paths(
    config_path: Path | None,
) -> tuple[Path, "BotfarmConfig | None"]:
    """Resolve database path from ``BOTFARM_DB_PATH`` env var and optionally load config.

    Returns (db_path, config_or_None).
    Raises ``click.ClickException`` if ``BOTFARM_DB_PATH`` is not set.
    """
    try:
        db_path = resolve_db_path()
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    config = None
    cfg_path = config_path or DEFAULT_CONFIG_PATH
    if cfg_path.exists():
        config = load_config(cfg_path)

    return db_path, config


def _format_duration(total_seconds: int) -> str:
    """Format a duration in seconds as a human-readable string (e.g. '2h30m', '5m30s', '45s')."""
    if total_seconds < 0:
        return "-"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def _elapsed(started_at: str | None) -> str:
    """Return a human-readable elapsed time string from an ISO timestamp."""
    if not started_at:
        return "-"
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - start
        return _format_duration(int(delta.total_seconds()))
    except (ValueError, TypeError):
        return "-"


_STATUS_COLORS = {
    "free": "green",
    "busy": "yellow",
    "paused_limit": "magenta",
    "failed": "red",
    "completed_pending_cleanup": "cyan",
}


@click.group()
@click.version_option(version=__version__)
def main():
    """Botfarm: autonomous Linear ticket dispatcher for Claude Code agents."""
    load_dotenv(ENV_FILE_PATH, override=False)
    cwd_env = Path.cwd() / ".env"
    if cwd_env.is_file() and cwd_env.resolve() != ENV_FILE_PATH.resolve():
        click.echo(
            f"Warning: found .env in current directory ({cwd_env}) "
            f"which is ignored. Botfarm only loads {ENV_FILE_PATH}.",
            err=True,
        )


@main.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def status(config_path):
    """Show current slot states across all projects."""
    try:
        db_path, config = _resolve_paths(config_path)
    except ConfigError as exc:
        click.echo(f"Config error: {exc}")
        return

    if not db_path.exists():
        cfg_path = config_path or DEFAULT_CONFIG_PATH
        if not cfg_path.exists():
            click.echo(
                "No database found and no config file found.\n"
                "Run `botfarm init` to create a default configuration."
            )
            return
        click.echo(
            "Config valid. No database yet — run `botfarm run` to start the supervisor."
        )
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        rows = load_all_slots(conn)
        paused, reason, _heartbeat = load_dispatch_state(conn)
        project_pauses = load_all_project_pause_states(conn)
    except sqlite3.OperationalError as exc:
        raise click.ClickException(f"Database query failed: {exc}") from exc
    finally:
        conn.close()

    if not rows:
        click.echo("No slots configured.")
        return

    console = Console()

    # Show dispatch pause banner if active
    if paused:
        if reason == "start_paused":
            console.print(
                "[bold yellow]DISPATCH PAUSED:[/bold yellow] "
                "waiting for user to start dispatching "
                "(run [bold]botfarm resume[/bold] to begin)\n"
            )
        else:
            console.print(f"[bold red]DISPATCH PAUSED:[/bold red] {reason or 'unknown'}\n")

    # Show per-project pause state
    paused_projects = {
        p: r for p, (is_paused, r) in project_pauses.items() if is_paused
    }
    if paused_projects:
        for proj, proj_reason in sorted(paused_projects.items()):
            label = f"PROJECT PAUSED: {proj}"
            if proj_reason:
                label += f" ({proj_reason})"
            console.print(f"[bold magenta]{label}[/bold magenta]")
        console.print()

    table = Table(title="Slot Status")
    table.add_column("Project", style="bold")
    table.add_column("Slot", justify="right")
    table.add_column("Status")
    table.add_column("Ticket")
    table.add_column("Stage")
    table.add_column("Elapsed")

    for row in rows:
        status_val = row["status"]
        color = _STATUS_COLORS.get(status_val, "white")
        ticket = row["ticket_id"] or "-"
        if row["ticket_title"] and row["ticket_id"]:
            ticket = f"{row['ticket_id']} ({row['ticket_title']})"
        stage = row["stage"] or "-"
        if (row["stage_iteration"] or 0) > 1:
            stage = f"{stage} (iter {row['stage_iteration']})"

        table.add_row(
            row["project"],
            str(row["slot_id"]),
            f"[{color}]{status_val}[/{color}]",
            ticket,
            stage,
            _elapsed(row["started_at"]),
        )

    console.print(table)


@main.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
@click.option("-n", "--limit", default=20, help="Number of tasks to show.")
@click.option("--project", default=None, help="Filter by project name.")
@click.option("--status", "status_filter", default=None, help="Filter by status.")
def history(config_path, limit, project, status_filter):
    """Show recent completed/failed tasks with key metrics."""
    db_path, _ = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. No tasks have been run yet.")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        rows = get_task_history(
            conn, limit=limit, project=project, status=status_filter
        )
        task_ids = [row["id"] for row in rows]
        agg = get_stage_run_aggregates(conn, task_ids) if task_ids else {}
        codex_stats = get_codex_review_stats(conn, task_ids) if task_ids else {}
    except sqlite3.OperationalError as exc:
        raise click.ClickException(f"Database query failed: {exc}") from exc
    finally:
        conn.close()

    if not rows:
        click.echo("No tasks found.")
        return

    console = Console()
    table = Table(title="Task History", show_lines=False)
    table.add_column("Ticket", style="bold", no_wrap=True)
    table.add_column("Project", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Turns", justify="right", no_wrap=True)
    table.add_column("Duration", justify="right", no_wrap=True)
    table.add_column("Cost", justify="right", no_wrap=True)
    table.add_column("Codex Cost", justify="right", no_wrap=True)
    table.add_column("Codex In/Out", justify="right", no_wrap=True)
    table.add_column("Codex", justify="center", no_wrap=True)
    table.add_column("Created", no_wrap=True)

    def _format_tokens(n: int) -> str:
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.0f}K"
        return str(n)

    for row in rows:
        status_val = row["status"]
        if status_val == "completed":
            status_display = "[green]completed[/green]"
        elif status_val == "failed":
            status_display = "[red]failed[/red]"
        elif status_val == "running":
            status_display = "[yellow]running[/yellow]"
        else:
            status_display = status_val

        turns = str(row["turns"]) if row["turns"] is not None else "-"

        duration = "-"
        if row["started_at"] and row["completed_at"]:
            try:
                start = datetime.fromisoformat(
                    row["started_at"].replace("Z", "+00:00")
                )
                end = datetime.fromisoformat(
                    row["completed_at"].replace("Z", "+00:00")
                )
                duration = _format_duration(int((end - start).total_seconds()))
            except (ValueError, TypeError):
                pass

        task_agg = agg.get(row["id"], {})
        cost = task_agg.get("total_cost_usd", 0.0)
        cost_display = f"${cost:.2f}" if cost else "-"

        task_codex = codex_stats.get(row["id"])
        if task_codex:
            codex_in = task_codex["codex_input_tokens"]
            codex_out = task_codex["codex_output_tokens"]
            codex_tokens_display = f"{_format_tokens(codex_in)}/{_format_tokens(codex_out)}"
            approved = task_codex["codex_approved"]
            total_runs = task_codex["codex_runs"]
            codex_verdict = f"[green]{approved}[/green]/{total_runs}"
        else:
            codex_tokens_display = "-"
            codex_verdict = "-"

        codex_cost = task_codex.get("codex_cost_usd", 0.0) if task_codex else 0.0
        codex_cost_display = f"${codex_cost:.2f}" if codex_cost else "-"

        created = row["created_at"][:16] if row["created_at"] else "-"

        table.add_row(
            row["ticket_id"],
            row["project"],
            status_display,
            turns,
            duration,
            cost_display,
            codex_cost_display,
            codex_tokens_display,
            codex_verdict,
            created,
        )

    console.print(table)


@main.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def limits(config_path):
    """Show current usage limit utilization."""
    db_path, cfg = _resolve_paths(config_path)

    # Load thresholds from config (fall back to defaults)
    from botfarm.config import UsageLimitsConfig

    if cfg is not None:
        threshold_5h = cfg.usage_limits.pause_five_hour_threshold
        threshold_7d = cfg.usage_limits.pause_seven_day_threshold
    else:
        defaults = UsageLimitsConfig()
        threshold_5h = defaults.pause_five_hour_threshold
        threshold_7d = defaults.pause_seven_day_threshold

    if not db_path.exists():
        click.echo("No database found. No usage data available.")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        # Refresh usage data from the API before displaying
        refresh_usage_snapshot(conn)

        row = conn.execute(
            "SELECT * FROM usage_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        # Read dispatch_paused from DB
        dispatch_paused, dispatch_pause_reason, _heartbeat = load_dispatch_state(conn)

        # Read Codex usage data
        codex_row = None
        try:
            codex_row = conn.execute(
                "SELECT * FROM codex_usage_snapshots ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            pass

        codex_stage_cost = 0.0
        try:
            cost_row = conn.execute(
                "SELECT SUM(total_cost_usd) as total "
                "FROM stage_runs WHERE stage = 'codex_review'"
            ).fetchone()
            if cost_row and cost_row["total"]:
                codex_stage_cost = cost_row["total"]
        except sqlite3.OperationalError:
            pass
    except sqlite3.OperationalError as exc:
        raise click.ClickException(f"Database query failed: {exc}") from exc
    finally:
        conn.close()

    if not row:
        click.echo("No usage snapshots recorded yet.")
        return

    console = Console()
    table = Table(title="Claude Usage Limits")
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    util_5h = row["utilization_5h"]
    util_7d = row["utilization_7d"]
    resets_at = row["resets_at"]
    resets_at_7d = row["resets_at_7d"]
    recorded_at = row["created_at"]

    # Derive yellow→red boundary as midpoint between threshold and 1.0
    red_5h = (threshold_5h + 1.0) / 2
    red_7d = (threshold_7d + 1.0) / 2

    if util_5h is not None:
        pct_5h = f"{util_5h * 100:.1f}%"
        color = "green" if util_5h < threshold_5h else ("yellow" if util_5h < red_5h else "red")
        table.add_row(
            f"5-hour utilization (pause >= {threshold_5h * 100:.0f}%)",
            f"[{color}]{pct_5h}[/{color}]",
        )
    else:
        table.add_row("5-hour utilization", "-")

    if util_7d is not None:
        pct_7d = f"{util_7d * 100:.1f}%"
        color = "green" if util_7d < threshold_7d else ("yellow" if util_7d < red_7d else "red")
        table.add_row(
            f"7-day utilization (pause >= {threshold_7d * 100:.0f}%)",
            f"[{color}]{pct_7d}[/{color}]",
        )
    else:
        table.add_row("7-day utilization", "-")

    if resets_at:
        table.add_row("5-hour resets at", resets_at)
    if resets_at_7d:
        table.add_row("7-day resets at", resets_at_7d)

    # Use dispatch_paused from DB if available, else derive from thresholds
    if dispatch_paused is not None:
        paused = dispatch_paused
    else:
        paused = (util_5h is not None and util_5h >= threshold_5h) or (
            util_7d is not None and util_7d >= threshold_7d
        )
    pause_color = "red" if paused else "green"
    pause_label = "YES" if paused else "no"
    if paused and dispatch_pause_reason:
        pause_label = f"YES — {dispatch_pause_reason}"
    table.add_row("Dispatch paused", f"[{pause_color}]{pause_label}[/{pause_color}]")

    if recorded_at:
        table.add_row("Last updated", recorded_at[:19])

    console.print(table)

    # Codex Usage section
    if codex_row or codex_stage_cost > 0:
        codex_table = Table(title="Codex Usage")
        codex_table.add_column("Metric", style="bold")
        codex_table.add_column("Value")

        if codex_row:
            monthly = codex_row["monthly_spend"]
            daily = codex_row["daily_spend"]
            budget = codex_row["monthly_budget"]
            utilization = codex_row["budget_utilization"]

            if monthly is not None:
                codex_table.add_row("Monthly spend", f"${monthly:.2f}")
            if daily is not None:
                codex_table.add_row("Daily spend", f"${daily:.2f}")
            if budget and budget > 0:
                codex_table.add_row("Monthly budget", f"${budget:.2f}")
                if utilization is not None:
                    pct = f"{utilization * 100:.1f}%"
                    color = "green" if utilization < 0.7 else ("yellow" if utilization < 0.9 else "red")
                    codex_table.add_row("Budget utilization", f"[{color}]{pct}[/{color}]")
            if codex_row["created_at"]:
                codex_table.add_row("Last polled", codex_row["created_at"][:19])

        if codex_stage_cost > 0:
            codex_table.add_row("Total cost (from runs)", f"${codex_stage_cost:.2f}")

        console.print(codex_table)


@main.command(name="pause")
@click.argument("project")
@click.option("--reason", default=None, help="Reason for pausing.")
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def pause_project(project, reason, config_path):
    """Pause dispatching new work for a specific project.

    Running workers are NOT interrupted — they finish their current task
    (all stages through merge/completion). Only new dispatches are skipped.
    """
    db_path, _ = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. Is the supervisor running?")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        # Verify the project exists in slot data
        rows = load_all_slots(conn)
        known_projects = {r["project"] for r in rows}
        if project not in known_projects:
            click.echo(
                f"Project '{project}' not found in database. "
                f"Known projects: {', '.join(sorted(known_projects)) or '(none)'}"
            )
            return

        save_project_pause_state(conn, project=project, paused=True, reason=reason)
        conn.commit()
        click.echo(f"Project '{project}' paused. Running workers will finish their current task.")
        if reason:
            click.echo(f"Reason: {reason}")
    except sqlite3.Error as exc:
        raise click.ClickException(f"Database error: {exc}") from exc
    finally:
        conn.close()


@main.command(name="resume")
@click.argument("project", required=False, default=None)
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def resume_project(project, config_path):
    """Resume dispatching new work.

    When called with a PROJECT argument, resumes dispatching for that project.
    When called without arguments, clears the global start_paused dispatch pause.
    """
    db_path, _ = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. Is the supervisor running?")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        if project is None:
            # Global resume: clear start_paused dispatch pause
            paused, reason, _heartbeat = load_dispatch_state(conn)
            if not paused:
                click.echo("Dispatch is not paused. Nothing to resume.")
                return
            if reason != "start_paused":
                click.echo(
                    f"Dispatch is paused for a different reason: {reason or 'unknown'}. "
                    "Use the dashboard or wait for automatic resolution."
                )
                return
            save_dispatch_state(conn, paused=False, supervisor_heartbeat=_heartbeat)
            conn.commit()
            click.echo("Dispatch resumed. Tickets will be dispatched on the next poll.")
            return

        # Per-project resume
        rows = load_all_slots(conn)
        known_projects = {r["project"] for r in rows}
        if project not in known_projects:
            click.echo(
                f"Project '{project}' not found in database. "
                f"Known projects: {', '.join(sorted(known_projects)) or '(none)'}"
            )
            return

        save_project_pause_state(conn, project=project, paused=False)
        conn.commit()
        click.echo(f"Project '{project}' resumed. New tickets will be dispatched on the next poll.")
    except sqlite3.Error as exc:
        raise click.ClickException(f"Database error: {exc}") from exc
    finally:
        conn.close()


DEFAULT_ENV_TEMPLATE = """\
# Botfarm environment variables
LINEAR_API_KEY=
# BOTFARM_DB_PATH=~/.botfarm/botfarm.db  # uncomment to override default
"""


def create_default_env(env_path: Path = ENV_FILE_PATH) -> Path:
    """Create a default .env file if it doesn't exist. Returns the path."""
    env_path.parent.mkdir(parents=True, exist_ok=True)
    if not env_path.exists():
        env_path.write_text(DEFAULT_ENV_TEMPLATE)
    return env_path


def _write_env_with_key(env_path: Path, api_key: str) -> None:
    """Write a .env file with the Linear API key filled in."""
    env_path.parent.mkdir(parents=True, exist_ok=True)
    content = (
        f"# Botfarm environment variables\n"
        f"LINEAR_API_KEY={api_key}\n"
        f"# BOTFARM_DB_PATH=~/.botfarm/botfarm.db  # uncomment to override default\n"
    )
    env_path.write_text(content)


def _generate_config_yaml(
    *,
    team_key: str,
    team_name: str,
    workspace: str,
) -> str:
    """Generate a config.yaml string with values filled in from the interactive flow."""
    return f"""\
# Botfarm configuration
# See documentation for full reference.

# Projects are configured per-repo via 'botfarm add-project'.
# Default team: {team_name} ({team_key})
# projects: []

linear:
  api_key: ${{LINEAR_API_KEY}}
  workspace: "{workspace}"
  poll_interval_seconds: 30
  exclude_tags:
    - Human
  issue_limit: 250
  todo_status: Todo
  in_progress_status: In Progress
  done_status: Done
  in_review_status: In Review
  comment_on_failure: true
  comment_on_completion: false
  comment_on_limit_pause: false
  capacity_monitoring:
    enabled: true
    warning_threshold: 0.70
    critical_threshold: 0.85
    pause_threshold: 0.95
    resume_threshold: 0.90

database:
  path: ""

usage_limits:
  enabled: true
  pause_five_hour_threshold: 0.85
  pause_seven_day_threshold: 0.90

dashboard:
  enabled: false
  host: 0.0.0.0
  port: 8420

agents:
  max_review_iterations: 3
  max_ci_retries: 2
  timeout_minutes:
    implement: 120
    review: 30
    fix: 60
  # timeout_overrides (first matching label wins; order matters):
  #   Investigation:
  #     implement: 30
  timeout_grace_seconds: 10
  codex_reviewer_enabled: false
  codex_reviewer_model: ""              # e.g. "o3", "o4-mini", or empty for default
  codex_reviewer_reasoning_effort: "medium"  # none, low, medium, high, xhigh — or empty for default
  codex_reviewer_timeout_minutes: 15    # separate from Claude review timeout
  codex_reviewer_skip_on_reiteration: true  # skip codex on review iterations 2+

# Separate coder/reviewer GitHub identities.
# Without this, all PRs and reviews use your personal GitHub account.
# With separate accounts, reviews appear as genuine third-party feedback.
# See docs/configuration.md for full setup instructions.
# identities:
#   coder:
#     github_token: ${{CODER_GITHUB_TOKEN}}          # Fine-grained PAT with repo write access
#     ssh_key_path: ~/.botfarm/coder_id_ed25519     # SSH key added to coder's GitHub account
#     git_author_name: "Coder Bot"
#     git_author_email: "coder-bot@example.com"
#   reviewer:
#     github_token: ${{REVIEWER_GITHUB_TOKEN}}        # Fine-grained PAT with PR read/write access

# Periodic refactoring analysis — auto-creates investigation tickets
# on a configurable cadence. Disabled by default.
# refactoring_analysis:
#   enabled: true
#   cadence_days: 14
#   linear_label: "Refactoring Analysis"
#   priority: 4  # Low priority — doesn't preempt feature work

# Note: this template should stay in sync with DEFAULT_CONFIG_TEMPLATE in config.py.
start_paused: true

# notifications:
#   webhook_url: https://hooks.slack.com/services/...
#   webhook_format: slack  # or "discord"
#   rate_limit_seconds: 300

# Daily work summary — sends a Claude-generated digest of the last 24h.
# daily_summary:
#   enabled: true
#   send_hour: 18  # UTC hour (0-23)
#   min_tasks_for_summary: 0  # 0 = always send
#   webhook_url: ""  # Falls back to notifications.webhook_url
"""


def _run_interactive_init(
    config_path: Path, env_path: Path, console: Console,
) -> bool:
    """Run the interactive init flow. Returns True if files were created."""
    from rich.prompt import Prompt

    console.print("\n[bold]Botfarm Setup[/bold]\n")

    # Step 1: Get Linear API key
    console.print("Enter your Linear API key (find it at linear.app → Settings → API).")
    api_key = Prompt.ask("Linear API key", password=True).strip()
    if not api_key:
        console.print("[red]API key cannot be empty.[/red]")
        return False

    # Step 2: Validate key and fetch teams
    console.print("\nValidating API key...", end=" ")
    client = LinearClient(api_key=api_key)
    try:
        teams = client.list_teams()
    except LinearAPIError as exc:
        console.print(f"[red]Failed![/red]\n  {exc}")
        return False

    if not teams:
        console.print("[red]No teams found for this API key.[/red]")
        return False
    console.print("[green]OK[/green]")

    # Step 3: Select team
    console.print(f"\n[bold]Teams ({len(teams)}):[/bold]")
    for i, team in enumerate(teams, 1):
        console.print(f"  {i}. {team['name']} ({team['key']})")

    if len(teams) == 1:
        selected_team = teams[0]
        console.print(f"\nAuto-selected: [bold]{selected_team['name']}[/bold]")
    else:
        choice = Prompt.ask(
            "Select team",
            choices=[str(i) for i in range(1, len(teams) + 1)],
        )
        selected_team = teams[int(choice) - 1]

    team_key = selected_team["key"]
    team_name = selected_team["name"]

    # Step 4: Auto-detect workspace slug
    console.print("\nDetecting workspace...", end=" ")
    try:
        org = client.get_organization()
        workspace = org["urlKey"]
        console.print(f"[green]{workspace}[/green]")
    except (LinearAPIError, KeyError):
        console.print("[yellow]could not auto-detect[/yellow]")
        workspace = Prompt.ask("Enter your Linear workspace slug")

    # Step 5: Generate files
    config_content = _generate_config_yaml(
        team_key=team_key,
        team_name=team_name,
        workspace=workspace,
    )

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(config_content)
    console.print(f"\n[green]Created config:[/green] {config_path}")

    _write_env_with_key(env_path, api_key)
    console.print(f"[green]Created .env:[/green]  {env_path}")

    # Summary
    console.print(f"\n[bold]Configuration summary:[/bold]")
    console.print(f"  Team:      {team_name} ({team_key})")
    console.print(f"  Workspace: {workspace}")
    console.print(
        "\n[dim]Next steps:[/dim]"
    )
    console.print(
        "[dim]  1. Run 'botfarm add-project' to configure your first project.[/dim]"
    )
    # TODO: Add an interactive identity setup prompt here.
    # Ask the user if they want to configure separate coder/reviewer
    # GitHub accounts, and if so, guide them through token creation
    # and SSH key generation. See docs/configuration.md for details.
    console.print(
        "[dim]  2. (Optional) Configure separate coder/reviewer identities "
        "— see docs/configuration.md.[/dim]"
    )
    return True


@main.command()
@click.option(
    "--path",
    type=click.Path(),
    default=None,
    help="Path for the config file.",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    default=False,
    help="Skip the interactive flow and just create template files.",
)
def init(path, non_interactive):
    """Set up botfarm configuration interactively.

    Connects to the Linear API to discover teams and workspace slug,
    then generates config.yaml and .env with the correct values.

    Use --non-interactive to just create template files without API calls.
    """
    config_path = DEFAULT_CONFIG_PATH if path is None else Path(path)

    if non_interactive:
        # Original non-interactive behavior
        created_config = False
        created_env = False

        if config_path.exists():
            click.echo(f"Config file already exists: {config_path}")
        else:
            create_default_config(config_path)
            click.echo(f"Created default config at: {config_path}")
            created_config = True

        if ENV_FILE_PATH.exists():
            click.echo(f".env file already exists: {ENV_FILE_PATH}")
        else:
            create_default_env(ENV_FILE_PATH)
            click.echo(f"Created default .env at: {ENV_FILE_PATH}")
            created_env = True

        if created_config or created_env:
            click.echo(
                f"\nNext step: set your Linear API key in {ENV_FILE_PATH}"
            )
        return

    # Interactive flow
    console = Console()

    env_path = ENV_FILE_PATH
    existing = []
    if config_path.exists():
        existing.append(str(config_path))
    if env_path.exists():
        existing.append(str(env_path))

    if existing:
        from rich.prompt import Confirm
        console.print(
            f"[yellow]Already exists:[/yellow] {', '.join(existing)}"
        )
        if not Confirm.ask("Overwrite?", default=False):
            return

    if not _run_interactive_init(config_path, env_path, console):
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# add-project command
# ---------------------------------------------------------------------------


def _run_readiness_checks(project_dict: dict) -> list[tuple[str, str]]:
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


def _append_project_to_config(config_path: Path, project_dict: dict) -> None:
    """Append a new project entry to the config.yaml projects list."""
    raw = config_path.read_text()
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    if "projects" not in data:
        data["projects"] = []
    elif not isinstance(data["projects"], list):
        raise ConfigError(
            "Config 'projects' key exists but is not a list — "
            "fix the config file manually before adding a project"
        )

    # Build the project entry — only include linear_project if non-empty
    entry: dict = {
        "name": project_dict["name"],
        "linear_team": project_dict["linear_team"],
        "base_dir": project_dict["base_dir"],
        "worktree_prefix": project_dict["worktree_prefix"],
        "slots": project_dict["slots"],
    }
    if project_dict.get("linear_project"):
        entry["linear_project"] = project_dict["linear_project"]

    data["projects"].append(entry)
    write_yaml_atomic(config_path, data)


def _extract_repo_name(repo_url: str) -> str:
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
    return url.split("/")[-1] or "my-project"


def _clone_repo(repo_url: str, target_dir: Path) -> None:
    """Clone a git repository to the target directory.

    Raises click.ClickException on failure.
    """
    try:
        result = subprocess.run(
            ["git", "clone", repo_url, str(target_dir)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            raise click.ClickException(
                f"git clone failed:\n{result.stderr.strip()}"
            )
    except FileNotFoundError:
        raise click.ClickException("git is not installed or not in PATH")
    except subprocess.TimeoutExpired:
        raise click.ClickException("git clone timed out after 5 minutes")


def _create_worktree(repo_dir: Path, worktree_path: Path, branch_name: str) -> None:
    """Create a git worktree with a new branch.

    Raises click.ClickException on failure.
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
            raise click.ClickException(
                f"Failed to create worktree at {worktree_path}:\n"
                f"{result.stderr.strip()}"
            )
    except subprocess.TimeoutExpired:
        raise click.ClickException(
            f"git worktree add timed out for {worktree_path}"
        )


@main.command("add-project")
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file (default: ~/.botfarm/config.yaml).",
)
def add_project(config_path):
    """Interactively add a new project with repo clone, worktrees, and config."""
    cfg_path = config_path or DEFAULT_CONFIG_PATH
    console = Console()

    if not cfg_path.exists():
        raise click.ClickException(
            f"Config file not found: {cfg_path}\n"
            "Run 'botfarm init' first to create a default config."
        )

    # Load existing config to check for duplicate names
    raw = cfg_path.read_text()
    data = yaml.safe_load(raw) or {}
    existing_projects = data.get("projects", [])
    existing_names = {
        p["name"] for p in existing_projects if isinstance(p, dict) and "name" in p
    }

    # Load the Linear API key from environment
    linear_api_key = os.environ.get("LINEAR_API_KEY", "")

    console.print("\n[bold]Add a new project to botfarm[/bold]\n")

    # --- 1. Repo URL ---
    repo_url = click.prompt("GitHub repo URL (SSH or HTTPS)")

    # --- 2. Project name (auto-suggest from repo URL) ---
    suggested_name = _extract_repo_name(repo_url)
    name = click.prompt("Project name", default=suggested_name)

    if name in existing_names:
        raise click.ClickException(
            f"Project '{name}' already exists in config. Choose a different name."
        )

    # --- 3. Linear team selection ---
    linear_team = ""
    selected_team = None
    client = None
    if linear_api_key:
        try:
            client = LinearClient(api_key=linear_api_key)
            teams = client.list_teams()
            if teams:
                console.print("\n[bold]Available Linear teams:[/bold]")
                for i, team in enumerate(teams, 1):
                    console.print(f"  {i}. {team['name']} ({team['key']})")
                choice = click.prompt(
                    "\nSelect team number",
                    type=click.IntRange(1, len(teams)),
                )
                selected_team = teams[choice - 1]
                linear_team = selected_team["key"]
            else:
                linear_team = click.prompt("Linear team key (e.g. SMA)")
        except LinearAPIError as exc:
            click.echo(f"Warning: Could not fetch teams from Linear: {exc}")
            linear_team = click.prompt("Linear team key (e.g. SMA)")
    else:
        click.echo("Warning: LINEAR_API_KEY not set. Linear selection will be manual.")
        linear_team = click.prompt("Linear team key (e.g. SMA)")

    # --- 4. Linear project selection (optional) ---
    linear_project = ""
    if client and selected_team:
        try:
            projects = client.list_team_projects(selected_team["id"])
            if projects:
                console.print("\n[bold]Available Linear projects:[/bold]")
                console.print("  0. (none — use all projects in team)")
                for i, proj in enumerate(projects, 1):
                    console.print(f"  {i}. {proj['name']}")
                choice = click.prompt(
                    "\nSelect project number",
                    type=click.IntRange(0, len(projects)),
                    default=0,
                )
                if choice > 0:
                    linear_project = projects[choice - 1]["name"]
        except LinearAPIError as exc:
            click.echo(f"Warning: Could not fetch projects from Linear: {exc}")
            linear_project = click.prompt(
                "Linear project filter (optional, press Enter to skip)", default=""
            )
    else:
        linear_project = click.prompt(
            "Linear project filter (optional, press Enter to skip)", default=""
        )

    # --- 5. Number of slots ---
    num_slots = click.prompt("Number of slots", type=click.IntRange(1, 20), default=1)
    slots = list(range(1, num_slots + 1))

    # --- Summary and confirmation ---
    projects_dir = DEFAULT_CONFIG_DIR / "projects" / name
    repo_dir = projects_dir / "repo"
    worktree_prefix = f"{name}-slot-"

    console.print("\n[bold]Summary:[/bold]")
    console.print(f"  Repo URL:       {repo_url}")
    console.print(f"  Project name:   {name}")
    console.print(f"  Linear team:    {linear_team}")
    if linear_project:
        console.print(f"  Linear project: {linear_project}")
    console.print(f"  Slots:          {num_slots}")
    console.print(f"  Clone to:       {repo_dir}")
    console.print(f"  Worktrees:      {projects_dir}/{worktree_prefix}<N>")

    if not click.confirm("\nProceed?", default=True):
        console.print("Aborted.")
        return

    # --- 6. Clone the repo ---
    if repo_dir.exists():
        raise click.ClickException(
            f"Directory already exists: {repo_dir}\n"
            f"Remove it first or choose a different project name."
        )

    # Track whether we created projects_dir so we can clean up on failure
    created_projects_dir = not projects_dir.exists()

    try:
        projects_dir.mkdir(parents=True, exist_ok=True)
        console.print(f"\n[bold]Cloning repository...[/bold]")
        _clone_repo(repo_url, repo_dir)
        console.print(f"  Cloned to {repo_dir}")

        # --- 7. Create worktrees and placeholder branches ---
        console.print(f"\n[bold]Creating {num_slots} worktree(s)...[/bold]")
        for slot_id in slots:
            branch_name = f"slot-{slot_id}-placeholder"
            worktree_path = projects_dir / f"{worktree_prefix}{slot_id}"
            _create_worktree(repo_dir, worktree_path, branch_name)
            console.print(f"  Slot {slot_id}: {worktree_path} (branch: {branch_name})")

        # --- 8. Update config.yaml ---
        console.print(f"\n[bold]Updating config...[/bold]")
        project_dict = {
            "name": name,
            "linear_team": linear_team,
            "base_dir": str(repo_dir),
            "worktree_prefix": str(projects_dir / worktree_prefix),
            "slots": slots,
            "linear_project": linear_project,
        }

        _append_project_to_config(cfg_path, project_dict)
    except (click.ClickException, ConfigError, OSError) as exc:
        console.print(f"\n[bold red]Error — cleaning up...[/bold red]")
        if created_projects_dir and projects_dir.exists():
            shutil.rmtree(projects_dir, ignore_errors=True)
            console.print(f"  Removed {projects_dir}")
        elif projects_dir.exists():
            # Only remove artifacts we created (repo + worktrees), not the parent dir
            if repo_dir.exists():
                shutil.rmtree(repo_dir, ignore_errors=True)
            for slot_id in slots:
                wt = projects_dir / f"{worktree_prefix}{slot_id}"
                if wt.exists():
                    shutil.rmtree(wt, ignore_errors=True)
            console.print(f"  Cleaned up cloned repo and worktrees from {projects_dir}")
        if isinstance(exc, click.ClickException):
            raise
        raise click.ClickException(f"Failed to write config: {exc}") from exc

    console.print(f"  Updated {cfg_path}")

    # --- Readiness checks (informational) ---
    readiness = _run_readiness_checks(project_dict)
    if readiness:
        console.print(f"\n[bold]Readiness checks:[/bold]")
        for level, msg in readiness:
            if level == "ok":
                console.print(f"  [green]OK[/green]   {msg}")
            else:
                console.print(f"  [yellow]WARN[/yellow] {msg}")

    # --- Done ---
    console.print(f"\n[bold green]Project '{name}' added successfully![/bold green]")
    console.print("\n  Next steps:")
    claude_md = repo_dir / "CLAUDE.md"
    if not claude_md.exists():
        console.print(
            f"    - Create a CLAUDE.md: botfarm init-claude-md {repo_dir}"
        )
    console.print("    - Start or restart the supervisor: botfarm run")


# ---------------------------------------------------------------------------
# Project detection helpers for init-claude-md
# ---------------------------------------------------------------------------

# Mapping of marker file -> (language, test command, dev setup command)
_PROJECT_MARKERS = [
    ("requirements.txt", "Python", "python -m pytest tests/", "pip install -r requirements.txt"),
    ("pyproject.toml", "Python", "python -m pytest tests/", "pip install -e ."),
    ("package.json", "Node.js", "npm test", "npm install"),
    ("go.mod", "Go", "go test ./...", "go mod download"),
    ("Cargo.toml", "Rust", "cargo test", "cargo build"),
    ("Gemfile", "Ruby", "bundle exec rspec", "bundle install"),
]


def _detect_project(project_dir: Path) -> dict:
    """Detect project characteristics from marker files.

    Returns a dict with keys: language, test_command, dev_command, marker.
    """
    for marker_file, language, test_cmd, dev_cmd in _PROJECT_MARKERS:
        if (project_dir / marker_file).exists():
            return {
                "language": language,
                "test_command": test_cmd,
                "dev_command": dev_cmd,
                "marker": marker_file,
            }
    return {
        "language": "Unknown",
        "test_command": "# Add your test command here",
        "dev_command": "# Add your dev setup command here",
        "marker": None,
    }


def _scan_readme(project_dir: Path) -> str:
    """Extract a project description from README.md.

    Returns the first non-empty, non-heading paragraph, or a placeholder.
    """
    for name in ("README.md", "readme.md", "README.rst", "README"):
        readme_path = project_dir / name
        if readme_path.exists():
            try:
                text = readme_path.read_text(errors="replace")
            except OSError:
                continue
            lines = text.splitlines()
            for i, line in enumerate(lines):
                stripped = line.strip()
                if not stripped:
                    continue
                # Skip markdown headings, badges (including linked [![), HTML tags
                if stripped.startswith(("#", "![", "[![", "<", "---", "===")):
                    continue
                # Skip RST underline/overline markers (lines of =, -, ~, etc.)
                if all(c in "=-~^\"'+`" for c in stripped):
                    continue
                # Skip RST titles (text followed by underline on the next line)
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and all(c in "=-~^\"'+`" for c in next_line):
                        continue
                # Truncate long descriptions
                if len(stripped) > 300:
                    stripped = stripped[:297] + "..."
                return stripped
    return "Describe your project here."


_SKIP_DIRS = {
    ".git", ".svn", ".hg", "node_modules", "__pycache__", ".venv", "venv",
    ".tox", ".mypy_cache", ".pytest_cache", "dist", "build", ".eggs",
    "target", "vendor", ".next", ".nuxt", "coverage", ".cache",
}


def _scan_directory_structure(project_dir: Path) -> str:
    """Build a concise top-level directory listing for the Architecture section."""
    entries = []
    try:
        items = sorted(project_dir.iterdir())
    except OSError:
        return "Describe key modules and patterns."

    for item in items:
        if item.name.startswith(".") and item.name != ".github":
            continue
        if item.name in _SKIP_DIRS:
            continue
        if item.is_dir():
            entries.append(f"- `{item.name}/`")
        # Only list notable top-level files
        elif item.name in (
            "Makefile", "Dockerfile", "docker-compose.yml", "docker-compose.yaml",
        ):
            entries.append(f"- `{item.name}`")

    if not entries:
        return "Describe key modules and patterns."
    # Cap at 20 entries to keep it manageable
    if len(entries) > 20:
        entries = entries[:20] + [f"- ... and {len(entries) - 20} more"]
    return "Top-level layout:\n" + "\n".join(entries)


_CI_FILES = [
    (".github/workflows", "GitHub Actions"),
    (".gitlab-ci.yml", "GitLab CI"),
    ("Jenkinsfile", "Jenkins"),
    (".circleci/config.yml", "CircleCI"),
    (".travis.yml", "Travis CI"),
    ("Makefile", "Makefile"),
    ("Dockerfile", "Docker"),
]


def _scan_ci_config(project_dir: Path) -> list[str]:
    """Detect CI/build tooling present in the repository."""
    found = []
    for path, label in _CI_FILES:
        if (project_dir / path).exists():
            found.append(label)
    return found


def _build_claude_md(project_dir: Path, detection: dict) -> str:
    """Assemble CLAUDE.md content from detection results and repo scanning."""
    project_name = project_dir.resolve().name
    description = _scan_readme(project_dir)
    architecture = _scan_directory_structure(project_dir)
    ci_tools = _scan_ci_config(project_dir)

    lines = [f"# {project_name}", ""]

    # Project Context
    lines.append("## Project Context")
    lines.append(description)
    lines.append("")

    # Testing
    lines.append("## Testing")
    lines.append("```bash")
    lines.append(detection["test_command"])
    lines.append("```")
    lines.append("")

    # Development
    lines.append("## Development")
    lines.append("```bash")
    lines.append(detection["dev_command"])
    lines.append("```")
    lines.append("")

    # CI / Build (only if something was detected)
    if ci_tools:
        lines.append("## CI / Build")
        lines.append("Detected: " + ", ".join(ci_tools))
        lines.append("")

    # Architecture
    lines.append("## Architecture")
    lines.append(architecture)
    lines.append("")

    # Conventions
    lines.append("## Conventions")
    lines.append("Describe coding standards and conventions.")
    lines.append("")

    return "\n".join(lines)


@main.command("init-claude-md")
@click.argument("project_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
def init_claude_md(project_dir):
    """Generate a CLAUDE.md template for a project directory."""
    claude_md_path = project_dir / "CLAUDE.md"

    if claude_md_path.exists():
        raise click.ClickException(f"CLAUDE.md already exists: {claude_md_path}")

    detection = _detect_project(project_dir)
    content = _build_claude_md(project_dir, detection)

    claude_md_path.write_text(content)

    if detection["marker"]:
        click.echo(f"Detected: {detection['language']} (found {detection['marker']})")
    else:
        click.echo("No known project markers detected — using generic template.")
    click.echo(f"Created: {claude_md_path}")


@main.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file (default: ~/.botfarm/config.yaml).",
)
@click.option(
    "--log-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory for log files (default: ~/.botfarm/logs/).",
)
@click.option(
    "--auto-restart/--no-auto-restart",
    default=True,
    help="Automatically restart after an update (exit code 42). Enabled by default.",
)
def run(config_path, log_dir, auto_restart):
    """Run the supervisor in foreground mode."""
    import sys

    from botfarm.git_update import UPDATE_EXIT_CODE
    from botfarm.supervisor import DEFAULT_LOG_DIR, Supervisor, setup_logging

    # Ensure ~/.local/bin is in PATH — non-login shells (nohup, systemd)
    # don't source .bashrc so Claude Code (typically installed there) would
    # not be found by workers.
    _ensure_local_bin_in_path()

    cfg_path = config_path or DEFAULT_CONFIG_PATH
    try:
        config = load_config(cfg_path)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc

    setup_logging(
        log_dir=log_dir or DEFAULT_LOG_DIR,
        console=True,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )

    supervisor = Supervisor(config, log_dir=log_dir or DEFAULT_LOG_DIR, auto_restart=auto_restart)
    exit_code = supervisor.run()

    if exit_code == UPDATE_EXIT_CODE and auto_restart:
        click.echo("Restarting after update...")
        os.execv(sys.executable, [sys.executable] + sys.argv)


# Derive "free" field values from the SlotState dataclass defaults so that
# adding a new field to SlotState automatically keeps reset in sync.
_free_state = SlotState(project="", slot_id=0)
_FREE_SLOT_FIELDS = {
    k: v for k, v in _free_state.to_dict().items()
    if k not in ("project", "slot_id")
}


@main.command()
@click.argument("project", required=False, default=None)
@click.option(
    "--all",
    "reset_all",
    is_flag=True,
    default=False,
    help="Reset all slots across all projects.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Skip confirmation and send SIGTERM to live processes without asking.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def reset(project, reset_all, force, config_path):
    """Reset stuck slots back to free for a given project.

    Resets all non-free slots for PROJECT to free, sending SIGTERM to any
    slots with live worker processes. Use --all to reset all projects.
    """
    if not project and not reset_all:
        raise click.UsageError(
            "Provide a project name or use --all to reset all projects."
        )

    db_path, _ = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. Nothing to reset.")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        try:
            rows = load_all_slots(conn)
        except sqlite3.OperationalError as exc:
            raise click.ClickException(
                f"Database query failed: {exc}"
            ) from exc

        slot_list = [dict(r) for r in rows]

        if not slot_list:
            click.echo("No slots found in database.")
            return

        # Check that the project exists if a specific project was given
        if project and not reset_all:
            known_projects = {s["project"] for s in slot_list}
            if project not in known_projects:
                click.echo(
                    f"Project '{project}' not found in database. "
                    f"Known projects: {', '.join(sorted(known_projects)) or '(none)'}"
                )
                return

        # Find non-free slots matching the filter
        targets = []
        for slot in slot_list:
            if slot.get("status", "free") == "free":
                continue
            if not reset_all and slot.get("project") != project:
                continue
            targets.append(slot)

        if not targets:
            scope = "all projects" if reset_all else f"project '{project}'"
            click.echo(
                f"All slots already free for {scope}. Nothing to reset."
            )
            return

        # Handle live PIDs
        live_pid_slots = [
            s for s in targets
            if s.get("pid") is not None and _is_pid_alive(s["pid"])
        ]

        if live_pid_slots and not force:
            click.echo("The following slots have live worker processes:")
            for s in live_pid_slots:
                click.echo(
                    f"  ({s.get('project')}, {s.get('slot_id')}) "
                    f"PID {s['pid']} — {s.get('ticket_id', '?')}"
                )
            if not click.confirm(
                "Send SIGTERM to these processes and reset slots?"
            ):
                click.echo("Aborted.")
                return

        # Send SIGTERM to live PIDs
        for s in live_pid_slots:
            pid = s["pid"]
            try:
                os.kill(pid, signal.SIGTERM)
                click.echo(f"  Sent SIGTERM to PID {pid}")
            except OSError:
                pass  # Process may have exited between check and signal

        # Reset slots
        console = Console()
        table = Table(title="Reset Slots")
        table.add_column("Project", style="bold")
        table.add_column("Slot", justify="right")
        table.add_column("Previous Status")
        table.add_column("Ticket")

        for slot in targets:
            prev_status = slot.get("status", "?")
            ticket = slot.get("ticket_id") or "-"
            table.add_row(
                slot.get("project", "?"),
                str(slot.get("slot_id", "?")),
                prev_status,
                ticket,
            )
            # Apply free_slot fields and write to DB
            slot.update(_FREE_SLOT_FIELDS)
            upsert_slot(conn, slot)
            clear_slot_stage(conn, slot["project"], slot["slot_id"])

        try:
            conn.commit()
        except sqlite3.Error as exc:
            raise click.ClickException(
                f"Failed to write database: {exc}"
            ) from exc

        console.print(table)
        click.echo(f"Reset {len(targets)} slot(s) to free.")
    finally:
        conn.close()


@main.command(name="install-service")
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file to pass to the service.",
)
@click.option(
    "--working-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Working directory for the service (default: current directory).",
)
@click.option(
    "--env-file",
    "env_files",
    type=click.Path(path_type=Path),
    multiple=True,
    help="Environment file(s) to include. May be repeated.",
)
def install_service_cmd(config_path, working_dir, env_files):
    """Install and enable a systemd user service for the botfarm supervisor.

    The service auto-restarts on crashes (non-zero exit, SIGKILL, etc.) but
    does NOT restart on a clean stop (exit code 0, SIGTERM, SIGINT).

    Requires: systemd with user session support and loginctl enable-linger
    for auto-start on boot.
    """
    env_file_list = list(env_files) if env_files else None

    # Preview the generated unit
    try:
        unit_content = generate_unit(
            config_path=config_path,
            working_dir=working_dir,
            env_files=env_file_list,
        )
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Unit file to be written to {UNIT_PATH}:\n")
    click.echo(unit_content)

    try:
        path = install_service(unit_content=unit_content)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"systemctl command failed: {exc.stderr or exc}"
        ) from exc

    click.echo(f"Service installed and enabled: {path}")
    click.echo(
        "\nTo start now:     systemctl --user start botfarm"
        "\nTo check status:  systemctl --user status botfarm"
        "\nTo view logs:     journalctl --user -u botfarm -f"
        "\nFor boot start:   loginctl enable-linger"
    )


@main.command(name="uninstall-service")
def uninstall_service_cmd():
    """Stop, disable, and remove the botfarm systemd user service."""
    if not UNIT_PATH.exists():
        click.echo("No botfarm service installed.")
        return

    try:
        uninstall_service()
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"systemctl command failed: {exc.stderr or exc}"
        ) from exc

    click.echo("Service stopped, disabled, and removed.")


def _days_ago(iso_timestamp: str | None) -> int:
    """Return how many days ago an ISO timestamp is, or 0 if unparseable."""
    if not iso_timestamp:
        return 0
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        return max(0, (datetime.now(timezone.utc) - dt).days)
    except (ValueError, TypeError):
        return 0


@main.command()
@click.option(
    "--action",
    type=click.Choice(["archive", "delete"]),
    default="archive",
    help="Action to perform (default: archive).",
)
@click.option(
    "--count",
    type=click.IntRange(min=1),
    default=50,
    help="Max issues to process (default: 50).",
)
@click.option(
    "--min-age",
    type=click.IntRange(min=0),
    default=7,
    help="Minimum days since completion (default: 7).",
)
@click.option(
    "--status",
    "status_filter",
    type=click.Choice(["done", "canceled", "all"]),
    default="all",
    help="Filter by status (default: all).",
)
@click.option("--project", default=None, help="Filter by project name.")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview candidates without taking action.",
)
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def cleanup(action, count, min_age, status_filter, project, dry_run, yes, config_path):
    """Bulk archive or delete old completed/canceled Linear issues."""
    db_path, cfg = _resolve_paths(config_path)

    if cfg is None:
        raise click.ClickException(
            "Config file not found. Run 'botfarm init' first."
        )

    if not cfg.linear.api_key:
        raise click.ClickException("linear.api_key not configured.")

    if not cfg.projects:
        raise click.ClickException("No projects configured.")

    console = Console()

    # Resolve team key and project name from config.
    # When --project is given, find the matching configured project so
    # we use the correct team; otherwise default to the first project.
    resolved_proj = cfg.projects[0]
    if project is not None:
        for p in cfg.projects:
            if p.linear_project == project or p.name == project:
                resolved_proj = p
                project = p.linear_project or p.name
                break
        else:
            console.print(
                f"[yellow]Warning: --project {project!r} does not match any "
                f"configured project. Using team {resolved_proj.linear_team!r}.[/yellow]"
            )
    else:
        project = resolved_proj.linear_project or ""
    team_key = resolved_proj.linear_team

    if not db_path.exists():
        raise click.ClickException(
            "No database found. Run the supervisor first to initialize the DB."
        )

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        client = LinearClient(api_key=cfg.linear.api_key)
        svc = CleanupService(
            client,
            conn,
            team_key=team_key,
            project_name=project or "",
            min_age_days=min_age,
            default_limit=count,
        )

        # Fetch candidates
        try:
            candidates = svc.fetch_candidates(
                limit=count, status_filter=status_filter
            )
        except LinearAPIError as exc:
            raise click.ClickException(f"Linear API error: {exc}") from exc

        if not candidates:
            console.print("No cleanup candidates found.")
            return

        # Display candidate table
        table = Table(title="Cleanup Candidates")
        table.add_column("ID", style="bold", no_wrap=True)
        table.add_column("Title")
        table.add_column("Status", no_wrap=True)
        table.add_column("Age (days)", justify="right", no_wrap=True)
        table.add_column("Project", no_wrap=True)

        for c in candidates:
            age = _days_ago(c.completed_at or c.updated_at)
            table.add_row(
                c.identifier,
                c.title[:60],
                c.status or "-",
                str(age),
                c.project_name or "-",
            )

        console.print(table)
        console.print(f"\n[bold]{len(candidates)}[/bold] candidate(s) found.\n")

        if dry_run:
            console.print("[dim]Dry run — no action taken.[/dim]")
            return

        # Confirmation prompt
        if not yes:
            if not click.confirm(
                f"{action.capitalize()} {len(candidates)} issue(s)?"
            ):
                console.print("Aborted.")
                return

        # Execute cleanup with progress (pass pre-fetched candidates to avoid
        # a redundant API call inside run_cleanup)
        with console.status(
            f"[bold yellow]{action.capitalize().rstrip('e')}ing {len(candidates)} issue(s)...",
        ):
            try:
                result = svc.run_cleanup(
                    action=action,
                    candidates=candidates,
                )
            except CooldownError as exc:
                raise click.ClickException(str(exc)) from exc
            except LinearAPIError as exc:
                raise click.ClickException(f"Linear API error: {exc}") from exc

        # Summary
        skipped_msg = ""
        if result.skipped:
            skipped_msg = f" {result.skipped} skipped (backup failed)."
        console.print(
            f"\n[bold green]{action.capitalize()}d {result.succeeded}/{result.total_candidates} issues successfully.[/bold green]"
            f"{skipped_msg}"
        )
        if result.failed:
            console.print(
                f"[bold red]{result.failed} failed.[/bold red]"
            )
            for err in result.errors[:5]:
                console.print(f"  [red]{err}[/red]")
    finally:
        conn.close()


@main.command(name="backfill-history")
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
@click.option("--project", default=None, help="Only backfill tickets from this project.")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-fetch even if ticket already exists in ticket_history.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be fetched without actually fetching.",
)
def backfill_history(config_path, project, force, dry_run):
    """Backfill ticket_history from Linear for all tickets in the tasks table."""
    from botfarm.linear import LinearAPIError, LinearClient

    db_path, config = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. No tasks to backfill.")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        # Get distinct ticket_ids from tasks table, optionally filtered by project
        query = "SELECT DISTINCT ticket_id FROM tasks"
        params: list[object] = []
        if project:
            query += " WHERE project = ?"
            params.append(project)
        query += " ORDER BY ticket_id"
        rows = conn.execute(query, params).fetchall()
        all_ticket_ids = [r[0] for r in rows]

        if not all_ticket_ids:
            click.echo("No tickets found in tasks table.")
            return

        # Determine which tickets need backfilling
        if force:
            to_fetch = all_ticket_ids
        else:
            to_fetch = [
                tid for tid in all_ticket_ids
                if get_ticket_history_entry(conn, tid) is None
            ]

        if not to_fetch:
            click.echo("All tickets already in ticket_history. Nothing to backfill.")
            return

        total = len(to_fetch)
        skipped_existing = len(all_ticket_ids) - len(to_fetch)

        if dry_run:
            click.echo(f"Dry run: would fetch {total} ticket(s) from Linear:")
            for tid in to_fetch:
                click.echo(f"  {tid}")
            if skipped_existing:
                click.echo(f"({skipped_existing} already in ticket_history, skipped)")
            return

        click.echo(f"Backfilling {total} ticket(s) from Linear...")
        if skipped_existing:
            click.echo(f"({skipped_existing} already in ticket_history, skipped)")

        if config is None:
            raise click.ClickException(
                "Config file required (needed for Linear API key). "
                "Use --config or ensure ~/.botfarm/config.yaml exists."
            )

        client = LinearClient(api_key=config.linear.api_key)
        fetched = 0
        failed = 0

        for i, ticket_id in enumerate(to_fetch, 1):
            try:
                details = client.fetch_issue_details(ticket_id)
                details["capture_source"] = "backfill"
                upsert_ticket_history(conn, **details)
                conn.commit()
                fetched += 1
            except LinearAPIError as exc:
                click.echo(
                    f"  Warning: failed to fetch {ticket_id} from Linear, skipping ({exc})"
                )
                failed += 1

            # Progress update every 5 tickets or on the last one
            if i % 5 == 0 or i == total:
                click.echo(f"  Processed {i}/{total} tickets...")

            # Rate limiting: small delay between API calls
            if i < total:
                time.sleep(0.2)

        click.echo(f"Backfill complete: {fetched} fetched, {failed} failed.")
    except sqlite3.Error as exc:
        raise click.ClickException(f"Database error: {exc}") from exc
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stop command helpers
# ---------------------------------------------------------------------------

_ACTIVE_STATUSES = frozenset({"busy", "paused_limit", "paused_manual"})
_STOP_GRACE_SECONDS = 5


def _slot_worktree_cwd(project_cfg, slot_id: int) -> str:
    """Compute the working directory for a slot's worktree."""
    base = Path(project_cfg.base_dir).expanduser()
    return str(base.parent / f"{project_cfg.worktree_prefix}{slot_id}")


def _slot_placeholder_branch(slot_id: int) -> str:
    return f"slot-{slot_id}-placeholder"


def _stop_kill_worker(pid: int) -> bool:
    """Kill a worker PID and its process group. Returns True if the process was signalled."""
    if not _is_pid_alive(pid):
        return False

    # Signal the process group first (covers child subprocesses), fall back
    # to the PID itself if the group call fails.
    try:
        os.killpg(pid, signal.SIGTERM)
    except (OSError, ProcessLookupError):
        try:
            os.kill(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            return False

    # Wait for graceful exit
    deadline = time.time() + _STOP_GRACE_SECONDS
    while _is_pid_alive(pid) and time.time() < deadline:
        time.sleep(0.5)

    # Escalate to SIGKILL if still alive.
    # Note: does not kill start_new_session=True subprocesses (Claude/Codex);
    # those are in separate sessions and will timeout on their own.
    if _is_pid_alive(pid):
        try:
            os.killpg(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            try:
                os.kill(pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass

    return True


def _stop_pr_cleanup(
    pr_url: str | None, cwd: str | None, env: dict[str, str] | None = None,
) -> tuple[bool, bool]:
    """Close PR if open, detect if merged. Returns (pr_was_merged, pr_closed)."""
    if not pr_url:
        return False, False

    # Check PR status via gh
    try:
        result = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json", "state"],
            capture_output=True, text=True, cwd=cwd, timeout=30, env=env,
        )
        if result.returncode != 0:
            return False, False

        pr_data = json.loads(result.stdout)
        state = pr_data.get("state", "").upper()
    except Exception:
        return False, False

    if state == "MERGED":
        return True, False

    if state == "OPEN":
        try:
            close_result = subprocess.run(
                ["gh", "pr", "close", pr_url],
                capture_output=True, text=True, cwd=cwd, timeout=30, env=env,
            )
            return False, close_result.returncode == 0
        except Exception:
            return False, False

    return False, False


def _stop_git_cleanup(
    cwd: str, slot_id: int, branch: str | None,
    env: dict[str, str] | None = None,
) -> tuple[bool, bool]:
    """Reset worktree to placeholder branch and delete feature branch.

    Returns ``(checkout_ok, branch_deleted)`` — *checkout_ok* is True when
    the worktree was successfully switched to the placeholder branch,
    *branch_deleted* is True when the local feature branch was removed.
    """
    placeholder = _slot_placeholder_branch(slot_id)
    branch_deleted = False

    # Remove stale index.lock
    lock_file = Path(cwd) / ".git" / "index.lock"
    if lock_file.exists():
        lock_file.unlink(missing_ok=True)

    # Reset and clean before checkout
    try:
        subprocess.run(
            ["git", "reset", "--hard"],
            capture_output=True, text=True, cwd=cwd, timeout=15, env=env,
        )
    except Exception:
        pass

    try:
        subprocess.run(
            ["git", "clean", "-fd"],
            capture_output=True, text=True, cwd=cwd, timeout=15, env=env,
        )
    except Exception:
        pass

    # Switch to placeholder branch
    checkout_ok = False
    try:
        result = subprocess.run(
            ["git", "checkout", placeholder],
            capture_output=True, text=True, cwd=cwd, timeout=15, env=env,
        )
        checkout_ok = result.returncode == 0
    except Exception:
        pass

    # Delete feature branch only after successful checkout
    if branch and not is_protected_branch(branch) and checkout_ok:
        try:
            del_result = subprocess.run(
                ["git", "branch", "-D", branch],
                capture_output=True, text=True, cwd=cwd, timeout=15, env=env,
            )
            branch_deleted = del_result.returncode == 0
        except Exception:
            pass

        # Delete remote branch (ignore errors)
        try:
            subprocess.run(
                ["git", "push", "origin", "--delete", branch],
                capture_output=True, text=True, cwd=cwd, timeout=30, env=env,
            )
        except Exception:
            pass

    return checkout_ok, branch_deleted


def _stop_linear_cleanup(
    config, ticket_id: str | None, project_name: str, pr_was_merged: bool,
) -> bool:
    """Move ticket back to todo (or done if merged) and post a comment.

    Returns True if the Linear state transition succeeded.
    """
    if not ticket_id or not config or not config.linear.api_key:
        return False

    # Find the project config to get team key
    project_cfg = None
    for p in config.projects:
        if p.name == project_name:
            project_cfg = p
            break
    if not project_cfg:
        return False

    try:
        client = LinearClient(api_key=config.linear.api_key)
        states = client.get_team_states(project_cfg.linear_team)

        if pr_was_merged:
            target = config.linear.done_status
        else:
            target = config.linear.todo_status

        state_id = states.get(target)
        if not state_id:
            return False
        client.update_issue_state(ticket_id, state_id)

        if not pr_was_merged:
            client.add_comment(ticket_id, "**Stopped by user** — work discarded")
        return True
    except Exception:
        return False  # Best-effort; don't fail the stop command


@main.command(name="stop")
@click.argument("project", required=False, default=None)
@click.argument("slot_id", required=False, default=None, type=int)
@click.option("--force", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--yes", "-y", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def stop(project, slot_id, force, yes, config_path):
    """Stop a running or paused slot, killing the worker and cleaning up.

    If PROJECT and SLOT_ID are not provided, shows an interactive selection
    of active slots.
    """
    skip_confirm = force or yes
    db_path, config = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. Is the supervisor running?")
        return

    try:
        conn = init_db(db_path)
    except SchemaVersionError as exc:
        raise click.ClickException(str(exc)) from exc
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        rows = load_all_slots(conn)
        active_slots = [
            dict(r) for r in rows if r["status"] in _ACTIVE_STATUSES
        ]

        if not active_slots:
            click.echo("No active slots to stop.")
            return

        # --- Resolve project ---
        if project is None:
            projects_with_active = sorted({s["project"] for s in active_slots})
            if len(projects_with_active) == 1:
                project = projects_with_active[0]
            else:
                # Interactive project selection
                click.echo("Active projects:")
                for i, p in enumerate(projects_with_active, 1):
                    count = sum(1 for s in active_slots if s["project"] == p)
                    click.echo(f"  [{i}] {p} ({count} active slot(s))")
                choice = click.prompt(
                    f"Select project [1-{len(projects_with_active)}]",
                    type=click.IntRange(1, len(projects_with_active)),
                )
                project = projects_with_active[choice - 1]

        # Filter to chosen project
        project_slots = [s for s in active_slots if s["project"] == project]
        if not project_slots:
            click.echo(f"No active slots for project '{project}'.")
            return

        # --- Resolve slot_id ---
        if slot_id is None:
            if len(project_slots) == 1:
                slot_id = project_slots[0]["slot_id"]
            else:
                # Interactive slot selection
                click.echo("\nActive slots:")
                for i, s in enumerate(project_slots, 1):
                    ticket = s.get("ticket_id") or "?"
                    title = s.get("ticket_title") or ""
                    stage = s.get("stage") or "?"
                    elapsed = _elapsed(s.get("started_at"))
                    title_display = f' "{title}"' if title else ""
                    click.echo(
                        f"  [{i}] {project} / slot {s['slot_id']} "
                        f"— {ticket}{title_display} "
                        f"(stage: {stage}, {elapsed} elapsed)"
                    )
                choice = click.prompt(
                    f"\nSelect slot to stop [1-{len(project_slots)}]",
                    type=click.IntRange(1, len(project_slots)),
                )
                slot_id = project_slots[choice - 1]["slot_id"]

        # Find the target slot
        target = None
        for s in active_slots:
            if s["project"] == project and s["slot_id"] == slot_id:
                target = s
                break

        if target is None:
            click.echo(f"Slot {project}/{slot_id} is not active.")
            return

        ticket_id = target.get("ticket_id")
        ticket_title = target.get("ticket_title") or ""
        branch = target.get("branch")
        stage = target.get("stage") or "?"
        pr_url = target.get("pr_url")
        pid = target.get("pid")
        elapsed = _elapsed(target.get("started_at"))

        # --- Confirmation prompt ---
        if not skip_confirm:
            title_display = f' "{ticket_title}"' if ticket_title else ""
            click.echo(f"\nStop slot {project}/{slot_id}?\n")
            click.echo(f"  Ticket: {ticket_id or '-'}{title_display}")
            click.echo(f"  Stage:  {stage} ({elapsed} elapsed)")
            if branch:
                click.echo(f"  Branch: {branch}")
            click.echo("\nThis will:")
            click.echo("  - Kill the running worker process")
            click.echo("  - Discard any uncommitted/unpushed work")
            if pr_url:
                click.echo("  - Close the PR without merging (if still open)")
            click.echo('  - Move the ticket back to "Todo"')
            if not click.confirm("\nContinue?", default=False):
                click.echo("Aborted.")
                return

        console = Console()

        # --- Kill worker process ---
        pid_killed = False
        if pid is not None:
            pid_killed = _stop_kill_worker(pid)

        # --- PR cleanup ---
        # Resolve worktree cwd and git env from config
        cwd = None
        subprocess_env = None
        if config:
            for p in config.projects:
                if p.name == project:
                    cwd = _slot_worktree_cwd(p, slot_id)
                    break
            git_env = build_git_env(config.identities)
            if git_env:
                subprocess_env = {**os.environ, **git_env}

        pr_was_merged, pr_closed = _stop_pr_cleanup(pr_url, cwd, env=subprocess_env)

        # --- Git cleanup ---
        # When the worktree cannot be resolved (config missing or directory
        # gone), treat as failure so the slot is NOT freed — mirrors the
        # supervisor which returns False and marks the slot as failed.
        checkout_ok = False
        branch_deleted = False
        if cwd and Path(cwd).is_dir():
            checkout_ok, branch_deleted = _stop_git_cleanup(
                cwd, slot_id, branch, env=subprocess_env,
            )

        # --- Linear cleanup ---
        linear_ok = _stop_linear_cleanup(config, ticket_id, project, pr_was_merged)

        # --- DB cleanup ---
        task_row = get_task_by_ticket(conn, ticket_id) if ticket_id else None
        task_id = task_row["id"] if task_row else None

        if task_id is not None:
            if pr_was_merged:
                update_task(
                    conn, task_id,
                    status="completed",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                )
            else:
                update_task(
                    conn, task_id,
                    status="failed",
                    failure_reason="stopped by user",
                )

        insert_event(
            conn,
            task_id=task_id,
            event_type="slot_stopped",
            detail=f"project={project}, slot={slot_id}, "
            f"ticket={ticket_id}, branch={branch}, "
            f"pr_merged={pr_was_merged}, pr_closed={pr_closed}, "
            f"checkout_ok={checkout_ok}, branch_deleted={branch_deleted}",
        )
        conn.commit()

        # Update slot in DB — mirror supervisor behavior.
        # Note: if the supervisor is running, it holds slot state in memory
        # and may overwrite this DB update on the next tick. However, the
        # worker PID is already dead, so the supervisor's reconciliation
        # loop will detect the dead process and handle cleanup itself.
        # This DB-only approach (Option A from the ticket spec) ensures
        # stop works even when the supervisor is not running.
        if checkout_ok:
            _free = SlotState(project="", slot_id=0)
            free_fields = {
                k: v for k, v in _free.to_dict().items()
                if k not in ("project", "slot_id")
            }
            slot_data = dict(target)
            slot_data.update(free_fields)
            upsert_slot(conn, slot_data)
            clear_slot_stage(conn, project, slot_id)
        else:
            # Don't free — worktree is still on the feature branch and
            # reusing it could cause the next ticket to inherit dirty state.
            # Only clear pid (mirrors mark_failed() which sets status="failed"
            # and pid=None, preserving ticket_id/title/branch for diagnosis).
            slot_data = dict(target)
            slot_data["status"] = "failed"
            slot_data["pid"] = None
            upsert_slot(conn, slot_data)
        conn.commit()

        # --- Post-stop output ---
        console.print(f"\n[bold green]Stopped slot {project}/{slot_id}[/bold green]")
        if pid_killed:
            console.print(f"  - Worker PID {pid} killed")
        if branch_deleted:
            console.print(f"  - Branch {branch} deleted")
        if pr_closed:
            console.print(f"  - PR closed")
        if pr_was_merged:
            console.print(
                f"  - [yellow]PR was already merged — you may need to manually revert[/yellow]"
            )
        if ticket_id and linear_ok:
            if pr_was_merged:
                console.print(f'  - Ticket {ticket_id} moved to "Done" (PR merged)')
            else:
                console.print(f'  - Ticket {ticket_id} moved to "Todo"')
        if not checkout_ok and cwd and Path(cwd).is_dir():
            console.print(
                f"  - [yellow]Warning: checkout to placeholder failed — "
                f"slot marked as failed[/yellow]"
            )
    except sqlite3.Error as exc:
        raise click.ClickException(f"Database error: {exc}") from exc
    finally:
        conn.close()
