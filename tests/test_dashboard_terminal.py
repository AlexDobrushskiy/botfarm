"""Tests for dashboard web terminal page and WebSocket endpoint."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from botfarm.config import DashboardConfig
from botfarm.dashboard import create_app
from botfarm.db import init_db


@pytest.fixture()
def db_file(tmp_path):
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
    conn.close()
    return path


@pytest.fixture()
def client(db_file):
    app = create_app(db_path=db_file)
    return TestClient(app)


@pytest.fixture()
def client_terminal_enabled(db_file):
    """Client with terminal_enabled=True in config."""
    cfg = MagicMock()
    cfg.dashboard = DashboardConfig(terminal_enabled=True)
    app = create_app(db_path=db_file, botfarm_config=cfg)
    return TestClient(app)


@pytest.fixture()
def client_terminal_disabled(db_file):
    """Client with terminal_enabled=False in config."""
    cfg = MagicMock()
    cfg.dashboard = DashboardConfig(terminal_enabled=False)
    app = create_app(db_path=db_file, botfarm_config=cfg)
    return TestClient(app)


class TestTerminalPage:
    def test_terminal_page_renders(self, client_terminal_enabled):
        resp = client_terminal_enabled.get("/terminal")
        assert resp.status_code == 200
        assert "xterm" in resp.text
        assert "terminal-container" in resp.text

    def test_terminal_page_has_websocket_url(self, client_terminal_enabled):
        resp = client_terminal_enabled.get("/terminal")
        assert resp.status_code == 200
        assert "/ws/terminal" in resp.text

    def test_terminal_page_disabled(self, client_terminal_disabled):
        resp = client_terminal_disabled.get("/terminal")
        assert resp.status_code == 403
        assert "disabled" in resp.text.lower()

    def test_terminal_page_disabled_by_default(self, client):
        resp = client.get("/terminal")
        assert resp.status_code == 403


class TestTerminalNavLink:
    def test_terminal_link_in_nav(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert 'href="/terminal"' in resp.text
        assert "Terminal" in resp.text


class TestTerminalConfigFlag:
    def test_default_config_terminal_disabled(self):
        cfg = DashboardConfig()
        assert cfg.terminal_enabled is False

    def test_config_terminal_enabled(self):
        cfg = DashboardConfig(terminal_enabled=True)
        assert cfg.terminal_enabled is True


class TestTerminalWebSocket:
    def test_ws_disabled_closes_with_4003(self, client_terminal_disabled):
        with pytest.raises(Exception):
            with client_terminal_disabled.websocket_connect("/ws/terminal"):
                pass  # Should close immediately with 4003

    def test_ws_session_limit(self, db_file):
        """Verify the session limit counter logic."""
        import asyncio

        from botfarm.dashboard.routes_terminal import (
            _decrement_sessions,
            _increment_sessions,
            _sessions_lock,
        )

        async def _test():
            # Reset state
            import botfarm.dashboard.routes_terminal as mod
            mod._active_sessions = 0

            # First two should succeed
            assert await _increment_sessions() is True
            assert await _increment_sessions() is True

            # Third should fail (max 2)
            assert await _increment_sessions() is False

            # After decrement, should succeed again
            await _decrement_sessions()
            assert await _increment_sessions() is True

            # Clean up
            mod._active_sessions = 0

        asyncio.run(_test())


class TestTerminalConfig:
    def test_config_parsing_terminal_enabled(self, tmp_path):
        """Test that terminal_enabled is parsed from YAML config."""
        from botfarm.config import load_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "bugtracker:\n"
            "  type: linear\n"
            "  api_key: test\n"
            "  workspace: test\n"
            "dashboard:\n"
            "  enabled: true\n"
            "  terminal_enabled: false\n"
        )
        env_file = tmp_path / ".env"
        env_file.write_text("")

        cfg = load_config(config_file)
        assert cfg.dashboard.terminal_enabled is False

    def test_config_parsing_terminal_default(self, tmp_path):
        """Test that terminal_enabled defaults to False."""
        from botfarm.config import load_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "bugtracker:\n"
            "  type: linear\n"
            "  api_key: test\n"
            "  workspace: test\n"
            "dashboard:\n"
            "  enabled: true\n"
        )
        env_file = tmp_path / ".env"
        env_file.write_text("")

        cfg = load_config(config_file)
        assert cfg.dashboard.terminal_enabled is False
