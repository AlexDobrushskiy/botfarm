"""Systemd user service management for the botfarm supervisor.

Generates, installs, and removes a ``~/.config/systemd/user/botfarm.service``
unit.  The supervisor runs with ``--auto-restart`` (the default) so that
dashboard-triggered updates work: after pulling new code the CLI calls
``os.execv()`` to replace the process in-place (same PID), which is
transparent to systemd.  Systemd's ``Restart=on-failure`` still catches
genuine crashes (non-zero exit, unclean signals).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

UNIT_NAME = "botfarm.service"
USER_UNIT_DIR = Path.home() / ".config" / "systemd" / "user"
UNIT_PATH = USER_UNIT_DIR / UNIT_NAME

_UNIT_TEMPLATE = """\
[Unit]
Description=Botfarm Supervisor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={exec_start}
WorkingDirectory={working_dir}
Restart=on-failure
RestartSec=5
KillMode=mixed
TimeoutStopSec=300
Environment=PATH={path}
{env_file_lines}
[Install]
WantedBy=default.target
"""


def _find_botfarm_bin() -> str:
    """Return the absolute path to the ``botfarm`` executable."""
    path = shutil.which("botfarm")
    if path:
        return path
    # Fallback: derive from current Python's bin directory
    bin_dir = Path(sys.executable).parent
    candidate = bin_dir / "botfarm"
    if candidate.exists():
        return str(candidate)
    raise FileNotFoundError(
        "Cannot find the 'botfarm' executable. "
        "Is the package installed (pip install -e .)?"
    )


def generate_unit(
    *,
    config_path: Path | None = None,
    working_dir: Path | None = None,
    env_files: list[Path] | None = None,
) -> str:
    """Generate the systemd unit file content.

    Parameters
    ----------
    config_path:
        Explicit config file path. Omitted from ExecStart when *None*
        (botfarm uses its default ``~/.botfarm/config.yaml``).
    working_dir:
        Service working directory. Defaults to the current directory.
    env_files:
        List of ``EnvironmentFile`` paths to include (e.g. ``~/.botfarm/.env``).
    """
    botfarm_bin = _find_botfarm_bin()

    def _quote(p: str) -> str:
        """Quote a path for systemd ExecStart if it contains spaces."""
        return f'"{p}"' if " " in p else p

    exec_parts = [_quote(botfarm_bin), "run"]
    if config_path is not None:
        exec_parts.extend(["--config", _quote(str(config_path))])

    wd = str((working_dir or Path.cwd()).resolve())

    env_lines = ""
    if env_files:
        for ef in env_files:
            resolved = ef.expanduser().resolve()
            env_lines += f"EnvironmentFile=-{resolved}\n"

    captured_path = os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")

    return _UNIT_TEMPLATE.format(
        exec_start=" ".join(exec_parts),
        working_dir=wd,
        path=captured_path,
        env_file_lines=env_lines,
    )


def install_service(
    *,
    config_path: Path | None = None,
    working_dir: Path | None = None,
    env_files: list[Path] | None = None,
    unit_content: str | None = None,
) -> Path:
    """Write the unit file, reload systemd, and enable the service.

    Returns the path to the installed unit file.

    Parameters
    ----------
    unit_content:
        Pre-generated unit file content.  When provided the other
        generation parameters are ignored and this content is written
        directly, avoiding a redundant ``generate_unit()`` call.
    """
    if unit_content is None:
        unit_content = generate_unit(
            config_path=config_path,
            working_dir=working_dir,
            env_files=env_files,
        )

    USER_UNIT_DIR.mkdir(parents=True, exist_ok=True)
    UNIT_PATH.write_text(unit_content)

    subprocess.run(
        ["systemctl", "--user", "daemon-reload"],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["systemctl", "--user", "enable", UNIT_NAME],
        check=True,
        capture_output=True,
        text=True,
    )

    return UNIT_PATH


def check_installed_unit_stale() -> tuple[bool, str]:
    """Check whether the installed unit file is stale.

    Returns ``(is_stale, message)``.  A unit is considered stale if it
    still contains ``--no-auto-restart`` (prevents dashboard-triggered
    updates), is missing an ``Environment=PATH=`` directive (child
    processes may not find binaries like codex, claude, or gh), or is
    missing ``KillMode=mixed`` / ``TimeoutStopSec`` directives
    (workers would be killed immediately on stop without cleanup).
    """
    if not UNIT_PATH.exists():
        return False, "no installed unit file"
    try:
        content = UNIT_PATH.read_text()
    except OSError as exc:
        return False, f"cannot read unit file: {exc}"
    if "--no-auto-restart" in content:
        return True, (
            f"installed unit {UNIT_PATH} contains --no-auto-restart — "
            "dashboard updates will not work. "
            "Run 'botfarm install-service' to regenerate the unit file."
        )
    if "Environment=PATH=" not in content:
        return True, (
            f"installed unit {UNIT_PATH} is missing Environment=PATH= directive — "
            "child processes may not find binaries like codex, claude, or gh. "
            "Run 'botfarm install-service' to regenerate the unit file."
        )
    if "KillMode=mixed" not in content:
        return True, (
            f"installed unit {UNIT_PATH} is missing KillMode=mixed — "
            "systemctl stop will kill workers immediately without cleanup. "
            "Run 'botfarm install-service' to regenerate the unit file."
        )
    if "TimeoutStopSec=" not in content:
        return True, (
            f"installed unit {UNIT_PATH} is missing TimeoutStopSec — "
            "workers may not have enough time to finish after supervisor exits. "
            "Run 'botfarm install-service' to regenerate the unit file."
        )
    return False, "OK"


def uninstall_service() -> None:
    """Stop, disable, and remove the systemd service."""
    # Stop (ignore errors if not running)
    subprocess.run(
        ["systemctl", "--user", "stop", UNIT_NAME],
        capture_output=True,
        text=True,
    )
    # Disable (ignore errors if not enabled)
    subprocess.run(
        ["systemctl", "--user", "disable", UNIT_NAME],
        capture_output=True,
        text=True,
    )

    if UNIT_PATH.exists():
        UNIT_PATH.unlink()

    subprocess.run(
        ["systemctl", "--user", "daemon-reload"],
        check=True,
        capture_output=True,
        text=True,
    )
