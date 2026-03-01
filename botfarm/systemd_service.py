"""Systemd user service management for the botfarm supervisor.

Generates, installs, and removes a ``~/.config/systemd/user/botfarm.service``
unit that auto-restarts the supervisor on crashes and signals while leaving
clean stops (exit code 0) alone.
"""

from __future__ import annotations

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

    exec_parts = [botfarm_bin, "run", "--no-auto-restart"]
    if config_path is not None:
        exec_parts.extend(["--config", str(config_path)])

    wd = str((working_dir or Path.cwd()).resolve())

    env_lines = ""
    if env_files:
        for ef in env_files:
            resolved = ef.expanduser().resolve()
            env_lines += f"EnvironmentFile=-{resolved}\n"

    return _UNIT_TEMPLATE.format(
        exec_start=" ".join(exec_parts),
        working_dir=wd,
        env_file_lines=env_lines,
    )


def install_service(
    *,
    config_path: Path | None = None,
    working_dir: Path | None = None,
    env_files: list[Path] | None = None,
) -> Path:
    """Write the unit file, reload systemd, and enable the service.

    Returns the path to the installed unit file.
    """
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
