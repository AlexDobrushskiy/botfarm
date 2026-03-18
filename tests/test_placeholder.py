from click.testing import CliRunner

from botfarm import __version__
from botfarm.cli import main


def test_version():
    assert __version__ == "0.1.0"


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Botfarm" in result.output


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_status_command(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
    runner = CliRunner()
    result = runner.invoke(main, ["status"])
    assert result.exit_code == 0


def test_history_command(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
    # Use a non-existent config to avoid depending on the system config
    monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", tmp_path / "config.yaml")
    runner = CliRunner()
    result = runner.invoke(main, ["history"])
    assert result.exit_code == 0


def test_limits_command(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
    monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", tmp_path / "config.yaml")
    runner = CliRunner()
    result = runner.invoke(main, ["limits"])
    assert result.exit_code == 0
