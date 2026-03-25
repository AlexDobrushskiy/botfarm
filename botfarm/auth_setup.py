"""Interactive authentication setup for the CLI.

Detects missing or expired authentication tokens and walks the user
through each required auth flow interactively.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

from botfarm.config import BotfarmConfig, DEFAULT_CONFIG_DIR, JiraBugtrackerConfig
from botfarm.credentials import CredentialError, _load_token

ENV_FILE_PATH = DEFAULT_CONFIG_DIR / ".env"


@dataclass
class AuthCheck:
    """Result of a single authentication check."""

    name: str
    label: str
    passed: bool
    message: str
    fixable: bool = True  # Whether we can guide the user to fix this


def check_claude_auth() -> AuthCheck:
    """Check whether Claude Code credentials are available."""
    if not shutil.which("claude"):
        local_claude = Path("~/.local/bin/claude").expanduser()
        if local_claude.exists():
            return AuthCheck(
                name="claude",
                label="Claude Code",
                passed=False,
                message=(
                    "'claude' found at ~/.local/bin/claude but not on PATH. "
                    "Add ~/.local/bin to your PATH."
                ),
                fixable=False,
            )
        return AuthCheck(
            name="claude",
            label="Claude Code",
            passed=False,
            message="'claude' binary not found. Install: curl -fsSL https://claude.ai/install.sh | bash",
            fixable=False,
        )

    try:
        _load_token()
    except CredentialError:
        return AuthCheck(
            name="claude",
            label="Claude Code",
            passed=False,
            message="Not authenticated — need to run 'claude' to log in",
        )

    return AuthCheck(
        name="claude",
        label="Claude Code",
        passed=True,
        message="Authenticated",
    )


def check_github_auth() -> AuthCheck:
    """Check whether GitHub CLI authentication is available."""
    if not shutil.which("gh"):
        return AuthCheck(
            name="github",
            label="GitHub CLI",
            passed=False,
            message="'gh' not found on PATH — install GitHub CLI first",
            fixable=False,
        )

    # Use `gh auth status` which handles all credential sources
    # (GH_TOKEN, GITHUB_TOKEN, hosts.yml, GH_CONFIG_DIR, etc.)
    try:
        result = subprocess.run(
            ["gh", "auth", "status"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return AuthCheck(
                name="github",
                label="GitHub CLI",
                passed=True,
                message="Authenticated",
            )
    except (subprocess.TimeoutExpired, OSError):
        pass

    return AuthCheck(
        name="github",
        label="GitHub CLI",
        passed=False,
        message="Not authenticated — need to run 'gh auth login'",
    )


def check_bugtracker_auth(config: BotfarmConfig | None) -> AuthCheck:
    """Check whether the bugtracker API key is configured and valid."""
    if config is None:
        return AuthCheck(
            name="bugtracker",
            label="Bugtracker",
            passed=False,
            message="No config loaded — run 'botfarm init' first",
            fixable=False,
        )

    bt = config.bugtracker
    is_jira = isinstance(bt, JiraBugtrackerConfig)
    tracker_name = "Jira" if is_jira else "Linear"

    if not bt.api_key:
        return AuthCheck(
            name="bugtracker",
            label=f"{tracker_name} API",
            passed=False,
            message=f"{tracker_name} API key not configured",
        )

    # Validate by calling the API
    from botfarm.bugtracker import BugtrackerError, create_client

    try:
        client = create_client(bt_config=bt)
        client.get_viewer_id()
    except Exception as exc:
        return AuthCheck(
            name="bugtracker",
            label=f"{tracker_name} API",
            passed=False,
            message=f"{tracker_name} API key invalid: {exc}",
        )

    return AuthCheck(
        name="bugtracker",
        label=f"{tracker_name} API",
        passed=True,
        message=f"Authenticated ({tracker_name})",
    )


def run_auth_checks(config: BotfarmConfig | None) -> list[AuthCheck]:
    """Run all authentication checks and return results."""
    return [
        check_claude_auth(),
        check_github_auth(),
        check_bugtracker_auth(config),
    ]


def display_auth_summary(console: Console, checks: list[AuthCheck]) -> None:
    """Display a summary table of authentication status."""
    table = Table(title="Authentication Status")
    table.add_column("Status", justify="center", width=6)
    table.add_column("Service", style="bold")
    table.add_column("Details")

    for check in checks:
        icon = "[green]\u2713[/green]" if check.passed else "[red]\u2717[/red]"
        table.add_row(icon, check.label, check.message)

    console.print(table)


def _fix_claude_auth(console: Console) -> bool:
    """Guide the user through Claude Code authentication."""
    console.print("\n[bold]Claude Code Authentication[/bold]")
    console.print("This will launch 'claude' which will open a browser for OAuth login.")
    console.print("Complete the login in your browser, then return here.\n")

    from rich.prompt import Confirm

    if not Confirm.ask("Launch claude login?", default=True):
        return False

    try:
        # Run claude in a way that triggers the login flow
        # claude -p with a simple prompt will trigger auth if needed
        proc = subprocess.run(
            ["claude", "-p", "echo hello"],
            timeout=120,
        )
        if proc.returncode == 0:
            # Verify credentials are now available
            try:
                _load_token()
                console.print("[green]Claude Code authentication successful![/green]")
                return True
            except CredentialError:
                console.print("[yellow]Claude ran but credentials not found. You may need to log in manually.[/yellow]")
                return False
        else:
            console.print("[red]Claude exited with an error. Try running 'claude' manually.[/red]")
            return False
    except subprocess.TimeoutExpired:
        console.print("[yellow]Timed out waiting for Claude login. Try running 'claude' manually.[/yellow]")
        return False
    except FileNotFoundError:
        console.print("[red]'claude' binary not found on PATH.[/red]")
        return False


def _fix_github_auth(console: Console) -> bool:
    """Guide the user through GitHub CLI authentication."""
    console.print("\n[bold]GitHub Authentication[/bold]")

    if not shutil.which("gh"):
        console.print("[red]GitHub CLI (gh) is not installed.[/red]")
        console.print("Install it from: https://cli.github.com/")
        return False

    console.print("This will run 'gh auth login' to authenticate with GitHub.")
    console.print("You can choose browser-based or token-based auth.\n")

    from rich.prompt import Confirm

    if not Confirm.ask("Run gh auth login?", default=True):
        return False

    try:
        # gh auth login is interactive — let it use the terminal directly
        proc = subprocess.run(
            ["gh", "auth", "login"],
            timeout=120,
        )
        if proc.returncode == 0:
            console.print("[green]GitHub authentication successful![/green]")
            return True
        else:
            console.print("[red]gh auth login failed. Try running it manually.[/red]")
            return False
    except subprocess.TimeoutExpired:
        console.print("[yellow]Timed out. Try running 'gh auth login' manually.[/yellow]")
        return False
    except FileNotFoundError:
        console.print("[red]'gh' binary not found on PATH.[/red]")
        return False


def _write_env_var(env_path: Path, key: str, value: str) -> None:
    """Append or update a variable in the .env file."""
    env_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    found = False
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith(f"{key}="):
                lines.append(f"{key}={value}")
                found = True
            else:
                lines.append(line)

    if not found:
        lines.append(f"{key}={value}")

    # Ensure trailing newline
    env_path.write_text("\n".join(lines) + "\n")


def _fix_bugtracker_auth(console: Console, config: BotfarmConfig | None) -> bool:
    """Guide the user through bugtracker API key setup."""
    console.print("\n[bold]Bugtracker Authentication[/bold]")

    if config is None:
        console.print("[red]No config loaded. Run 'botfarm init' first.[/red]")
        return False

    bt = config.bugtracker
    is_jira = isinstance(bt, JiraBugtrackerConfig)
    tracker_name = "Jira" if is_jira else "Linear"

    from rich.prompt import Prompt

    if is_jira:
        console.print(f"Enter your Jira API token.")
        console.print("Generate one at: https://id.atlassian.com/manage-profile/security/api-tokens")
    else:
        console.print(f"Enter your Linear API key.")
        console.print("Find it at: https://linear.app → Settings → API → Personal API keys")

    api_key = Prompt.ask(f"\n{tracker_name} API key", password=True).strip()
    if not api_key:
        console.print("[red]API key cannot be empty.[/red]")
        return False

    # Validate the key
    console.print("Validating...", end=" ")
    from botfarm.bugtracker import BugtrackerError, create_client

    try:
        if is_jira:
            client = create_client(
                api_key=api_key,
                bugtracker_type="jira",
                bt_config=bt,
            )
        else:
            client = create_client(api_key=api_key, bugtracker_type="linear")
        client.get_viewer_id()
    except Exception as exc:
        console.print(f"[red]Failed![/red]\n  {exc}")
        return False

    console.print("[green]OK[/green]")

    # Save to .env and update in-memory state so re-check picks up the new key
    env_var = "JIRA_API_TOKEN" if is_jira else "LINEAR_API_KEY"
    env_path = ENV_FILE_PATH
    try:
        _write_env_var(env_path, env_var, api_key)
        os.environ[env_var] = api_key
        bt.api_key = api_key
        console.print(f"Saved {env_var} to {env_path}")
    except OSError as exc:
        console.print(f"[red]Failed to write .env: {exc}[/red]")
        console.print(f"Set {env_var} manually in {env_path}")
        return False

    return True


def run_interactive_auth(
    config: BotfarmConfig | None,
    console: Console,
    *,
    fix_all: bool = False,
) -> bool:
    """Run auth checks and interactively fix failures.

    When *fix_all* is True, automatically attempt to fix all failures
    without asking whether to proceed.

    Returns True if all checks pass (or were fixed).
    """
    from rich.prompt import Confirm

    checks = run_auth_checks(config)
    display_auth_summary(console, checks)

    failed = [c for c in checks if not c.passed]
    if not failed:
        console.print("\n[bold green]All authentication checks passed![/bold green]")
        return True

    fixable = [c for c in failed if c.fixable]
    unfixable = [c for c in failed if not c.fixable]

    if unfixable:
        console.print(f"\n[yellow]{len(unfixable)} issue(s) require manual action:[/yellow]")
        for check in unfixable:
            console.print(f"  - {check.label}: {check.message}")

    if not fixable:
        return False

    console.print(f"\n[bold]{len(fixable)} issue(s) can be fixed interactively.[/bold]")

    _FIX_HANDLERS = {
        "claude": _fix_claude_auth,
        "github": _fix_github_auth,
        "bugtracker": _fix_bugtracker_auth,
    }

    fixed_count = 0
    for check in fixable:
        handler = _FIX_HANDLERS.get(check.name)
        if handler is None:
            continue

        if not fix_all:
            if not Confirm.ask(f"\nFix {check.label}?", default=True):
                continue

        if check.name == "bugtracker":
            success = handler(console, config)
        else:
            success = handler(console)

        if success:
            fixed_count += 1

    # Re-check and display final status
    console.print("\n")
    final_checks = run_auth_checks(config)
    display_auth_summary(console, final_checks)

    all_passed = all(c.passed for c in final_checks)
    if all_passed:
        console.print("\n[bold green]All authentication checks passed![/bold green]")
    else:
        still_failed = sum(1 for c in final_checks if not c.passed)
        console.print(
            f"\n[bold yellow]{still_failed} check(s) still failing. "
            f"Fix manually and re-run 'botfarm auth'.[/bold yellow]"
        )

    return all_passed
