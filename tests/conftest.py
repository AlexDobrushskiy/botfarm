"""Shared pytest fixtures for botfarm unit tests."""

from __future__ import annotations

import pytest

from botfarm.db import init_db


@pytest.fixture()
def conn(tmp_path):
    """Yield a fresh DB connection with all migrations applied."""
    db_file = tmp_path / "test.db"
    connection = init_db(db_file, allow_migration=True)
    yield connection
    connection.close()
