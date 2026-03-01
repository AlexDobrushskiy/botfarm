import os
import signal
import sqlite3
import subprocess
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
    get_task_history,
    init_db,
    load_all_project_pause_states,
    load_all_slots,
    load_dispatch_state,
    resolve_db_path,
    save_dispatch_state,
    save_project_pause_state,
    upsert_slot,
)
from botfarm.slots import SlotState, _is_pid_alive
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
    except sqlite3.OperationalError as exc:
        raise click.ClickException(f"Database query failed: {exc}") from exc
    finally:
        conn.close()

    if not rows:
        click.echo("No tasks found.")
        return

    console = Console()
    table = Table(title="Task History")
    table.add_column("Ticket", style="bold")
    table.add_column("Project")
    table.add_column("Status")
    table.add_column("Turns", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Created")

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

        created = row["created_at"][:16] if row["created_at"] else "-"

        table.add_row(
            row["ticket_id"],
            row["project"],
            status_display,
            turns,
            duration,
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
@click.argument("project")
@click.option(
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path to config file.",
)
def resume_project(project, config_path):
    """Resume dispatching new work for a paused project."""
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

    The service auto-restarts on crashes and signals (SIGTERM, SIGINT) but
    does NOT restart on a clean stop (exit code 0).

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
        path = install_service(
            config_path=config_path,
            working_dir=working_dir,
            env_files=env_file_list,
        )
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
