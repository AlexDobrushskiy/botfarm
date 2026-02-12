from pathlib import Path

import click
from dotenv import load_dotenv

from botfarm import __version__
from botfarm.config import (
    DEFAULT_CONFIG_DIR,
    DEFAULT_CONFIG_PATH,
    ConfigError,
    create_default_config,
    load_config,
)

ENV_FILE_PATH = DEFAULT_CONFIG_DIR / ".env"


@click.group()
@click.version_option(version=__version__)
def main():
    """Botfarm: autonomous Linear ticket dispatcher for Claude Code agents."""
    load_dotenv(ENV_FILE_PATH, override=False)


@main.command()
def status():
    """Show current agent status."""
    click.echo("No agents running.")


@main.command()
def history():
    """Show task execution history."""
    click.echo("No history yet.")


@main.command()
def limits():
    """Show current rate limit status."""
    click.echo("No limit data available.")


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
