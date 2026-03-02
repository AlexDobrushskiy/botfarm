"""Tests for botfarm.systemd_service and the install/uninstall CLI commands."""

import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from botfarm.cli import main
from botfarm.systemd_service import (
    UNIT_NAME,
    _find_botfarm_bin,
    generate_unit,
    install_service,
    uninstall_service,
)


# ---------------------------------------------------------------------------
# _find_botfarm_bin
# ---------------------------------------------------------------------------


class TestFindBotfarmBin:
    def test_finds_via_which(self, monkeypatch):
        monkeypatch.setattr("botfarm.systemd_service.shutil.which", lambda _: "/usr/bin/botfarm")
        assert _find_botfarm_bin() == "/usr/bin/botfarm"

    def test_fallback_to_sys_executable_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("botfarm.systemd_service.shutil.which", lambda _: None)
        fake_bin = tmp_path / "botfarm"
        fake_bin.touch()
        monkeypatch.setattr("botfarm.systemd_service.sys.executable", str(tmp_path / "python"))
        assert _find_botfarm_bin() == str(fake_bin)

    def test_raises_when_not_found(self, tmp_path, monkeypatch):
        monkeypatch.setattr("botfarm.systemd_service.shutil.which", lambda _: None)
        monkeypatch.setattr("botfarm.systemd_service.sys.executable", str(tmp_path / "python"))
        with pytest.raises(FileNotFoundError, match="Cannot find"):
            _find_botfarm_bin()


# ---------------------------------------------------------------------------
# generate_unit
# ---------------------------------------------------------------------------


class TestGenerateUnit:
    @pytest.fixture(autouse=True)
    def _mock_bin(self, monkeypatch):
        monkeypatch.setattr(
            "botfarm.systemd_service._find_botfarm_bin",
            lambda: "/opt/venv/bin/botfarm",
        )

    def test_basic_unit(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "Description=Botfarm Supervisor" in unit
        assert "Restart=on-failure" in unit
        assert "RestartSec=5" in unit
        assert "--no-auto-restart" not in unit
        assert "WantedBy=default.target" in unit
        assert f"WorkingDirectory={tmp_path}" in unit
        assert "ExecStart=/opt/venv/bin/botfarm run" in unit

    def test_with_config_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        unit = generate_unit(config_path=Path("/etc/botfarm/config.yaml"))
        assert "--config /etc/botfarm/config.yaml" in unit

    def test_without_config_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "--config" not in unit

    def test_with_working_dir(self):
        unit = generate_unit(working_dir=Path("/home/user/mybot"))
        assert "WorkingDirectory=/home/user/mybot" in unit

    def test_with_env_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        env1 = tmp_path / ".env"
        env2 = tmp_path / "shared.env"
        unit = generate_unit(env_files=[env1, env2])
        assert f"EnvironmentFile=-{env1}" in unit
        assert f"EnvironmentFile=-{env2}" in unit

    def test_no_env_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "EnvironmentFile" not in unit


# ---------------------------------------------------------------------------
# install_service
# ---------------------------------------------------------------------------


class TestInstallService:
    @pytest.fixture(autouse=True)
    def _mock_bin(self, monkeypatch):
        monkeypatch.setattr(
            "botfarm.systemd_service._find_botfarm_bin",
            lambda: "/opt/venv/bin/botfarm",
        )

    def test_writes_unit_and_runs_systemctl(self, tmp_path, monkeypatch):
        unit_dir = tmp_path / "systemd" / "user"
        unit_path = unit_dir / UNIT_NAME
        monkeypatch.setattr("botfarm.systemd_service.USER_UNIT_DIR", unit_dir)
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        monkeypatch.chdir(tmp_path)

        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr("botfarm.systemd_service.subprocess.run", fake_run)

        result = install_service()
        assert result == unit_path
        assert unit_path.exists()

        content = unit_path.read_text()
        assert "Restart=on-failure" in content

        # Verify systemctl commands were called
        assert ["systemctl", "--user", "daemon-reload"] in calls
        assert ["systemctl", "--user", "enable", UNIT_NAME] in calls

    def test_creates_parent_dirs(self, tmp_path, monkeypatch):
        unit_dir = tmp_path / "deep" / "nested" / "dir"
        unit_path = unit_dir / UNIT_NAME
        monkeypatch.setattr("botfarm.systemd_service.USER_UNIT_DIR", unit_dir)
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "botfarm.systemd_service.subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0),
        )

        install_service()
        assert unit_path.exists()


# ---------------------------------------------------------------------------
# uninstall_service
# ---------------------------------------------------------------------------


class TestUninstallService:
    def test_removes_unit_and_runs_systemctl(self, tmp_path, monkeypatch):
        unit_dir = tmp_path / "systemd" / "user"
        unit_dir.mkdir(parents=True)
        unit_path = unit_dir / UNIT_NAME
        unit_path.write_text("[Unit]\nDescription=test\n")
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)

        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr("botfarm.systemd_service.subprocess.run", fake_run)

        uninstall_service()
        assert not unit_path.exists()
        assert ["systemctl", "--user", "stop", UNIT_NAME] in calls
        assert ["systemctl", "--user", "disable", UNIT_NAME] in calls
        assert ["systemctl", "--user", "daemon-reload"] in calls

    def test_no_error_when_unit_missing(self, tmp_path, monkeypatch):
        unit_path = tmp_path / UNIT_NAME
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        monkeypatch.setattr(
            "botfarm.systemd_service.subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0),
        )

        # Should not raise
        uninstall_service()


# ---------------------------------------------------------------------------
# CLI: botfarm install-service
# ---------------------------------------------------------------------------


class TestInstallServiceCommand:
    @pytest.fixture()
    def runner(self):
        return CliRunner()

    def test_install_service_success(self, runner, tmp_path, monkeypatch):
        unit_dir = tmp_path / "systemd" / "user"
        unit_path = unit_dir / UNIT_NAME
        monkeypatch.setattr("botfarm.systemd_service.USER_UNIT_DIR", unit_dir)
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        # Also patch the UNIT_PATH imported into cli module
        monkeypatch.setattr("botfarm.cli.UNIT_PATH", unit_path)
        monkeypatch.setattr(
            "botfarm.systemd_service._find_botfarm_bin",
            lambda: "/usr/bin/botfarm",
        )
        monkeypatch.setattr(
            "botfarm.systemd_service.subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0),
        )
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(main, ["install-service"])
        assert result.exit_code == 0
        assert "Service installed and enabled" in result.output
        assert "systemctl --user start botfarm" in result.output
        assert unit_path.exists()

    def test_install_service_with_config(self, runner, tmp_path, monkeypatch):
        unit_dir = tmp_path / "systemd" / "user"
        unit_path = unit_dir / UNIT_NAME
        monkeypatch.setattr("botfarm.systemd_service.USER_UNIT_DIR", unit_dir)
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        monkeypatch.setattr("botfarm.cli.UNIT_PATH", unit_path)
        monkeypatch.setattr(
            "botfarm.systemd_service._find_botfarm_bin",
            lambda: "/usr/bin/botfarm",
        )
        monkeypatch.setattr(
            "botfarm.systemd_service.subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0),
        )
        monkeypatch.chdir(tmp_path)

        cfg = tmp_path / "config.yaml"
        cfg.touch()
        result = runner.invoke(main, ["install-service", "--config", str(cfg)])
        assert result.exit_code == 0
        content = unit_path.read_text()
        assert f"--config {cfg}" in content

    def test_install_service_with_env_files(self, runner, tmp_path, monkeypatch):
        unit_dir = tmp_path / "systemd" / "user"
        unit_path = unit_dir / UNIT_NAME
        monkeypatch.setattr("botfarm.systemd_service.USER_UNIT_DIR", unit_dir)
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        monkeypatch.setattr("botfarm.cli.UNIT_PATH", unit_path)
        monkeypatch.setattr(
            "botfarm.systemd_service._find_botfarm_bin",
            lambda: "/usr/bin/botfarm",
        )
        monkeypatch.setattr(
            "botfarm.systemd_service.subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0),
        )
        monkeypatch.chdir(tmp_path)

        env1 = tmp_path / ".env"
        env1.touch()
        result = runner.invoke(main, ["install-service", "--env-file", str(env1)])
        assert result.exit_code == 0
        content = unit_path.read_text()
        assert f"EnvironmentFile=-{env1}" in content

    def test_install_service_botfarm_not_found(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr("botfarm.systemd_service.shutil.which", lambda _: None)
        monkeypatch.setattr(
            "botfarm.systemd_service.sys.executable", str(tmp_path / "python")
        )
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(main, ["install-service"])
        assert result.exit_code != 0
        assert "Cannot find" in result.output


# ---------------------------------------------------------------------------
# CLI: botfarm uninstall-service
# ---------------------------------------------------------------------------


class TestUninstallServiceCommand:
    @pytest.fixture()
    def runner(self):
        return CliRunner()

    def test_uninstall_service_success(self, runner, tmp_path, monkeypatch):
        unit_dir = tmp_path / "systemd" / "user"
        unit_dir.mkdir(parents=True)
        unit_path = unit_dir / UNIT_NAME
        unit_path.write_text("[Unit]\nDescription=test\n")
        monkeypatch.setattr("botfarm.systemd_service.UNIT_PATH", unit_path)
        monkeypatch.setattr("botfarm.cli.UNIT_PATH", unit_path)
        monkeypatch.setattr(
            "botfarm.systemd_service.subprocess.run",
            lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0),
        )

        result = runner.invoke(main, ["uninstall-service"])
        assert result.exit_code == 0
        assert "stopped, disabled, and removed" in result.output
        assert not unit_path.exists()

    def test_uninstall_service_not_installed(self, runner, tmp_path, monkeypatch):
        unit_path = tmp_path / UNIT_NAME
        monkeypatch.setattr("botfarm.cli.UNIT_PATH", unit_path)

        result = runner.invoke(main, ["uninstall-service"])
        assert result.exit_code == 0
        assert "No botfarm service installed" in result.output


# ---------------------------------------------------------------------------
# Unit content validation
# ---------------------------------------------------------------------------


class TestUnitContentSemantics:
    """Verify that the generated unit has the right systemd semantics."""

    @pytest.fixture(autouse=True)
    def _mock_bin(self, monkeypatch):
        monkeypatch.setattr(
            "botfarm.systemd_service._find_botfarm_bin",
            lambda: "/opt/venv/bin/botfarm",
        )

    def test_restart_on_failure_not_always(self, tmp_path, monkeypatch):
        """on-failure means clean exit (code 0) won't restart."""
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "Restart=on-failure" in unit
        assert "Restart=always" not in unit

    def test_auto_restart_enabled(self, tmp_path, monkeypatch):
        """Default --auto-restart lets dashboard updates use os.execv in-place."""
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "--no-auto-restart" not in unit

    def test_user_service_target(self, tmp_path, monkeypatch):
        """User services use default.target, not multi-user.target."""
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "WantedBy=default.target" in unit
        assert "multi-user.target" not in unit

    def test_type_simple(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        unit = generate_unit()
        assert "Type=simple" in unit

    def test_env_file_uses_dash_prefix(self, tmp_path, monkeypatch):
        """Dash prefix means systemd won't fail if the file is missing."""
        monkeypatch.chdir(tmp_path)
        unit = generate_unit(env_files=[Path("/home/user/.botfarm/.env")])
        assert "EnvironmentFile=-" in unit
