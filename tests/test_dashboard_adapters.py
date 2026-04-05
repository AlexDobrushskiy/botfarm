"""Tests for the dashboard adapter management page (/config/adapters)."""

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import (
    AdapterConfig,
    AgentsConfig,
    BotfarmConfig,
    LinearBugtrackerConfig,
)
from botfarm.dashboard import create_app
from botfarm.db import init_db


@pytest.fixture()
def db_file(tmp_path):
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
    conn.close()
    return path


def _make_config(tmp_path, adapters=None, source_path=""):
    agents = AgentsConfig()
    if adapters is not None:
        agents.adapters = adapters
    cfg = BotfarmConfig(
        projects=[],
        bugtracker=LinearBugtrackerConfig(),
        agents=agents,
        source_path=source_path,
    )
    return cfg


class _FakeCheckResult:
    def __init__(self, name, passed, message, critical=True):
        self.name = name
        self.passed = passed
        self.message = message
        self.critical = critical


# ---------------------------------------------------------------------------
# GET /config/adapters
# ---------------------------------------------------------------------------


class TestAdaptersPageGET:
    def test_returns_200(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/config/adapters")
        assert resp.status_code == 200
        assert "Adapters" in resp.text

    def test_lists_default_adapters(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/config/adapters")
        body = resp.text
        assert "claude" in body
        assert "codex" in body

    def test_shows_enabled_badge(self, db_file, tmp_path):
        cfg = _make_config(tmp_path, adapters={
            "claude": AdapterConfig(enabled=True),
        })
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/config/adapters")
        assert "badge-enabled" in resp.text

    def test_shows_disabled_badge(self, db_file, tmp_path):
        cfg = _make_config(tmp_path, adapters={
            "codex": AdapterConfig(enabled=False),
        })
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/config/adapters")
        assert "badge-disabled" in resp.text

    def test_shows_preflight_results(self, db_file, tmp_path):
        cfg = _make_config(tmp_path, adapters={
            "claude": AdapterConfig(enabled=True),
        })
        results = [
            _FakeCheckResult("adapter:claude:available", True, "Claude CLI found"),
            _FakeCheckResult("adapter:claude:auth", False, "OAuth expired", critical=True),
        ]
        app = create_app(
            db_path=db_file,
            botfarm_config=cfg,
            get_preflight_results=lambda: results,
        )
        client = TestClient(app)
        resp = client.get("/config/adapters")
        body = resp.text
        assert "Claude CLI found" in body
        assert "OAuth expired" in body

    def test_no_config_shows_message(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config/adapters")
        assert resp.status_code == 200
        assert "Configuration is not available" in resp.text

    def test_nav_link_present(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/")
        assert 'href="/config/adapters"' in resp.text


# ---------------------------------------------------------------------------
# POST /config/adapters
# ---------------------------------------------------------------------------


class TestAdaptersPagePOST:
    def test_saves_adapter_config(self, db_file, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(yaml.dump({
            "agents": {"adapters": {"claude": {"enabled": True, "model": ""}}},
        }))
        cfg = _make_config(tmp_path, source_path=str(cfg_path))
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)

        resp = client.post("/config/adapters", json={
            "claude": {
                "enabled": True,
                "model": "claude-sonnet-4-6",
                "timeout_minutes": 60,
                "reasoning_effort": "",
                "skip_on_reiteration": True,
            },
        })
        assert resp.status_code == 200
        assert "success" in resp.text

        # Verify in-memory config updated
        assert cfg.agents.adapters["claude"].model == "claude-sonnet-4-6"
        assert cfg.agents.adapters["claude"].timeout_minutes == 60

        # Verify YAML persisted
        raw = yaml.safe_load(cfg_path.read_text())
        assert raw["agents"]["adapters"]["claude"]["model"] == "claude-sonnet-4-6"

    def test_disable_adapter(self, db_file, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(yaml.dump({
            "agents": {"adapters": {"claude": {"enabled": True}}},
        }))
        cfg = _make_config(tmp_path, adapters={
            "claude": AdapterConfig(enabled=True),
        }, source_path=str(cfg_path))
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)

        resp = client.post("/config/adapters", json={
            "claude": {"enabled": False},
        })
        assert resp.status_code == 200
        assert not cfg.agents.adapters["claude"].enabled

    def test_validation_rejects_bad_timeout(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)

        resp = client.post("/config/adapters", json={
            "claude": {"timeout_minutes": -5},
        })
        assert resp.status_code == 422
        assert "at least 1" in resp.text

    def test_validation_rejects_bad_effort(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)

        resp = client.post("/config/adapters", json={
            "claude": {"reasoning_effort": "supermax"},
        })
        assert resp.status_code == 422
        assert "reasoning_effort" in resp.text

    def test_no_config_returns_400(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/config/adapters", json={"claude": {"enabled": True}})
        assert resp.status_code == 400

    def test_invalid_json_returns_400(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.post(
            "/config/adapters",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400

    def test_null_timeout_clears_value(self, db_file, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(yaml.dump({
            "agents": {"adapters": {"codex": {"enabled": False, "timeout_minutes": 15}}},
        }))
        cfg = _make_config(tmp_path, adapters={
            "codex": AdapterConfig(enabled=False, timeout_minutes=15),
        }, source_path=str(cfg_path))
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)

        resp = client.post("/config/adapters", json={
            "codex": {"timeout_minutes": None},
        })
        assert resp.status_code == 200
        assert cfg.agents.adapters["codex"].timeout_minutes is None


# ---------------------------------------------------------------------------
# POST /api/adapter-preflight
# ---------------------------------------------------------------------------


class TestAdapterPreflightAPI:
    def test_triggers_callback(self, db_file, tmp_path):
        called = []
        cfg = _make_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=cfg,
            on_rerun_preflight=lambda: called.append("rerun"),
        )
        client = TestClient(app)
        resp = client.post("/api/adapter-preflight")
        assert resp.status_code == 200
        assert called == ["rerun"]

    def test_works_without_callback(self, db_file, tmp_path):
        cfg = _make_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.post("/api/adapter-preflight")
        assert resp.status_code == 200
