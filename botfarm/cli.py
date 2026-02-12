import json
import sqlite3
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

ENV_FILE_PATH = DEFAULT_CONFIG_DIR / ".env"

# Default paths (match config.py defaults)
DEFAULT_STATE_FILE = DEFAULT_CONFIG_DIR / "state.json"
DEFAULT_DB_PATH = DEFAULT_CONFIG_DIR / "botfarm.db"


def _resolve_paths(
    config_path: Path | None,
) -> tuple[Path, Path]:
    """Resolve state file and database paths from config, falling back to defaults."""
    state_path = DEFAULT_STATE_FILE
    db_path = DEFAULT_DB_PATH

    cfg_path = config_path or DEFAULT_CONFIG_PATH
    if cfg_path.exists():
        try:
            config = load_config(cfg_path)
            state_path = Path(config.state_file).expanduser()
            db_path = Path(config.database.path).expanduser()
        except (ConfigError, Exception):
            pass

    return state_path, db_path


def _elapsed(started_at: str | None) -> str:
    """Return a human-readable elapsed time string from an ISO timestamp."""
    if not started_at:
        return "-"
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - start
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0:
            return "-"
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h{minutes:02d}m"
        if minutes:
            return f"{minutes}m{seconds:02d}s"
        return f"{seconds}s"
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
    state_path, _ = _resolve_paths(config_path)

    if not state_path.exists():
        click.echo("No state file found. Is the supervisor running?")
        return

    try:
        data = json.loads(state_path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        raise click.ClickException(f"Failed to read state file: {exc}") from exc

    if not isinstance(data, list) or not data:
        click.echo("No slots configured.")
        return

    console = Console()
    table = Table(title="Slot Status")
    table.add_column("Project", style="bold")
    table.add_column("Slot", justify="right")
    table.add_column("Status")
    table.add_column("Ticket")
    table.add_column("Stage")
    table.add_column("Elapsed")

    for slot in data:
        status_val = slot.get("status", "unknown")
        color = _STATUS_COLORS.get(status_val, "white")
        ticket = slot.get("ticket_id") or "-"
        if slot.get("ticket_title") and slot.get("ticket_id"):
            ticket = f"{slot['ticket_id']} ({slot['ticket_title']})"
        stage = slot.get("stage") or "-"
        if slot.get("stage_iteration", 0) > 1:
            stage = f"{stage} (iter {slot['stage_iteration']})"

        table.add_row(
            slot.get("project", "?"),
            str(slot.get("slot_id", "?")),
            f"[{color}]{status_val}[/{color}]",
            ticket,
            stage,
            _elapsed(slot.get("started_at")),
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
    _, db_path = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. No tasks have been run yet.")
        return

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        query = "SELECT * FROM tasks WHERE 1=1"
        params: list[object] = []
        if project is not None:
            query += " AND project = ?"
            params.append(project)
        if status_filter is not None:
            query += " AND status = ?"
            params.append(status_filter)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
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
    table.add_column("Cost ($)", justify="right")
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

        cost = f"{row['cost_usd']:.2f}" if row["cost_usd"] else "-"
        turns = str(row["turns"]) if row["turns"] else "-"

        duration = "-"
        if row["started_at"] and row["completed_at"]:
            try:
                start = datetime.fromisoformat(
                    row["started_at"].replace("Z", "+00:00")
                )
                end = datetime.fromisoformat(
                    row["completed_at"].replace("Z", "+00:00")
                )
                delta = end - start
                total_secs = int(delta.total_seconds())
                hours, remainder = divmod(total_secs, 3600)
                minutes, secs = divmod(remainder, 60)
                if hours:
                    duration = f"{hours}h{minutes:02d}m"
                elif minutes:
                    duration = f"{minutes}m{secs:02d}s"
                else:
                    duration = f"{secs}s"
            except (ValueError, TypeError):
                pass

        created = row["created_at"][:16] if row["created_at"] else "-"

        table.add_row(
            row["ticket_id"],
            row["project"],
            status_display,
            cost,
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
    _, db_path = _resolve_paths(config_path)

    if not db_path.exists():
        click.echo("No database found. No usage data available.")
        return

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        raise click.ClickException(f"Failed to open database: {exc}") from exc

    try:
        row = conn.execute(
            "SELECT * FROM usage_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
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
    recorded_at = row["created_at"]

    if util_5h is not None:
        pct_5h = f"{util_5h * 100:.1f}%"
        color = "green" if util_5h < 0.7 else ("yellow" if util_5h < 0.9 else "red")
        table.add_row("5-hour utilization", f"[{color}]{pct_5h}[/{color}]")
    else:
        table.add_row("5-hour utilization", "-")

    if util_7d is not None:
        pct_7d = f"{util_7d * 100:.1f}%"
        color = "green" if util_7d < 0.7 else ("yellow" if util_7d < 0.9 else "red")
        table.add_row("7-day utilization", f"[{color}]{pct_7d}[/{color}]")
    else:
        table.add_row("7-day utilization", "-")

    if resets_at:
        table.add_row("Resets at", resets_at)

    paused = util_5h is not None and util_5h >= 0.9
    pause_color = "red" if paused else "green"
    pause_label = "YES" if paused else "no"
    table.add_row("Dispatch paused", f"[{pause_color}]{pause_label}[/{pause_color}]")

    if recorded_at:
        table.add_row("Last updated", recorded_at[:19])

    console.print(table)


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
def run(config_path, log_dir):
    """Run the supervisor in foreground mode."""
    from botfarm.supervisor import Supervisor, setup_logging

    cfg_path = config_path or DEFAULT_CONFIG_PATH
    try:
        config = load_config(cfg_path)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from exc

    setup_logging(log_dir=log_dir, console=True)

    supervisor = Supervisor(config, log_dir=log_dir)
    supervisor.run()
