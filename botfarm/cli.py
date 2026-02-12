import click

from botfarm import __version__


@click.group()
@click.version_option(version=__version__)
def main():
    """Botfarm: autonomous Linear ticket dispatcher for Claude Code agents."""
    pass


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
