"""Tests for dotenv loading in botfarm CLI."""

import os

from click.testing import CliRunner

from botfarm.cli import ENV_FILE_PATH, main


def test_env_file_path():
    """ENV_FILE_PATH points to ~/.botfarm/.env."""
    assert ENV_FILE_PATH.name == ".env"
    assert ENV_FILE_PATH.parent.name == ".botfarm"


def test_dotenv_loads_env_vars(tmp_path, monkeypatch):
    """CLI loads variables from .env file into os.environ."""
    import botfarm.cli as cli_mod

    db_path = tmp_path / "nonexistent.db"
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"BOTFARM_TEST_DOTENV_VAR=loaded_value\n"
        f"BOTFARM_DB_PATH={db_path}\n"
    )
    monkeypatch.setattr(cli_mod, "ENV_FILE_PATH", env_file)
    monkeypatch.delenv("BOTFARM_TEST_DOTENV_VAR", raising=False)
    monkeypatch.delenv("BOTFARM_DB_PATH", raising=False)

    runner = CliRunner()
    result = runner.invoke(main, ["status"])
    try:
        assert result.exit_code == 0
        assert os.environ.get("BOTFARM_TEST_DOTENV_VAR") == "loaded_value"
    finally:
        os.environ.pop("BOTFARM_TEST_DOTENV_VAR", None)


def test_dotenv_no_error_when_missing(tmp_path, monkeypatch):
    """CLI does not error when .env file does not exist."""
    import botfarm.cli as cli_mod

    monkeypatch.setattr(cli_mod, "ENV_FILE_PATH", tmp_path / "nonexistent" / ".env")
    # BOTFARM_DB_PATH must still be set for CLI to work
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))

    runner = CliRunner()
    result = runner.invoke(main, ["status"])
    assert result.exit_code == 0


def test_dotenv_does_not_override_existing_env(tmp_path, monkeypatch):
    """Existing environment variables are not overridden by .env file."""
    import botfarm.cli as cli_mod

    env_file = tmp_path / ".env"
    env_file.write_text("BOTFARM_TEST_EXISTING=from_dotenv\n")
    monkeypatch.setattr(cli_mod, "ENV_FILE_PATH", env_file)
    monkeypatch.setenv("BOTFARM_TEST_EXISTING", "from_env")
    # BOTFARM_DB_PATH must be set for CLI to work
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))

    runner = CliRunner()
    result = runner.invoke(main, ["status"])
    assert result.exit_code == 0
    assert os.environ["BOTFARM_TEST_EXISTING"] == "from_env"
