"""Shared pytest fixtures for botfarm unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from botfarm.db import init_db
from botfarm.linear import PollResult
from botfarm.supervisor import Supervisor
from tests.helpers import make_config


@pytest.fixture()
def conn(tmp_path):
    """Yield a fresh DB connection with all migrations applied."""
    db_file = tmp_path / "test.db"
    connection = init_db(db_file, allow_migration=True)
    yield connection
    connection.close()


@pytest.fixture()
def tmp_config(tmp_path):
    """Return a BotfarmConfig and ensure directories exist."""
    config = make_config(tmp_path)
    (tmp_path / "repo").mkdir()
    return config


@pytest.fixture()
def supervisor(tmp_config, tmp_path, monkeypatch):
    """Create a Supervisor with mocked pollers (no real Linear calls)."""
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
    mock_poller.is_issue_terminal.return_value = False

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

    return sup
