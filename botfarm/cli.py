from pathlib import Path

import click
from dotenv import load_dotenv

from botfarm import __version__
from botfarm.config import DEFAULT_CONFIG_DIR, DEFAULT_CONFIG_PATH, create_default_config

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
