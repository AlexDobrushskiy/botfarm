import json
import os
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

from botfarm import __version__
from botfarm.config import (
    DEFAULT_CONFIG_DIR,
    DEFAULT_CONFIG_PATH,
    ConfigError,
    create_default_config,
    load_config,
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
    # Load local .env first (slot-specific, e.g. BOTFARM_DB_PATH),
    # then global ~/.botfarm/.env (shared, e.g. LINEAR_API_KEY).
    # override=False means first-loaded value wins.
    load_dotenv(Path.cwd() / ".env", override=False)
    load_dotenv(ENV_FILE_PATH, override=False)


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

        created = row["created_at"][:16] if row["created_at"] else "-"

        table.add_row(
            row["ticket_id"],
            row["project"],
            status_display,
            turns,
            duration,
            cost_display,
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
    except sqlite3.OperationalError as exc:
        raise click.ClickException(f"Database query failed: {exc}") from exc
    finally:
        conn.close()

    if not row:
        click.echo("No usage snapshots recorded yet.")
        return

    console = Console()
    table = Table(title="Usage Limits")
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


@main.command()
@click.option(
    "--path",
    type=click.Path(),
    default=None,
    help="Path for the config file.",
)
def init(path):
    """Create a default configuration file."""
    config_path = DEFAULT_CONFIG_PATH if path is None else Path(path)
    if config_path.exists():
        click.echo(f"Config file already exists: {config_path}")
        return
    create_default_config(config_path)
    click.echo(f"Created default config at: {config_path}")


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
