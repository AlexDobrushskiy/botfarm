"""Tests for runtime_config DB table and sync helpers."""

from __future__ import annotations

from botfarm.config import AgentsConfig, sync_agent_config_to_db
from botfarm.db import (
    RUNTIME_CONFIG_KEYS,
    read_runtime_config,
    write_runtime_config,
    write_runtime_config_batch,
)


class TestRuntimeConfigRoundTrip:
    """Round-trip for each type (bool, str, int)."""

    def test_bool_round_trip(self, conn):
        write_runtime_config(conn, "codex_reviewer_enabled", True)
        conn.commit()
        result = read_runtime_config(conn)
        assert result["codex_reviewer_enabled"] is True

    def test_str_round_trip(self, conn):
        write_runtime_config(conn, "codex_reviewer_model", "o3")
        conn.commit()
        result = read_runtime_config(conn)
        assert result["codex_reviewer_model"] == "o3"

    def test_int_round_trip(self, conn):
        write_runtime_config(conn, "max_review_iterations", 5)
        conn.commit()
        result = read_runtime_config(conn)
        assert result["max_review_iterations"] == 5


class TestRuntimeConfigUpsert:
    """Upsert behavior (write same key twice)."""

    def test_upsert_overwrites(self, conn):
        write_runtime_config(conn, "max_ci_retries", 2)
        conn.commit()
        write_runtime_config(conn, "max_ci_retries", 4)
        conn.commit()
        result = read_runtime_config(conn)
        assert result["max_ci_retries"] == 4


class TestRuntimeConfigBatch:
    """Batch write + read back."""

    def test_batch_write_and_read(self, conn):
        settings = {
            "max_review_iterations": 3,
            "max_ci_retries": 2,
            "max_merge_conflict_retries": 2,
            "codex_reviewer_enabled": False,
            "codex_reviewer_model": "o4-mini",
            "codex_reviewer_timeout_minutes": 15,
        }
        write_runtime_config_batch(conn, settings)
        result = read_runtime_config(conn)
        assert result == settings


class TestSyncAgentConfigToDb:
    """sync_agent_config_to_db verifies all 5 keys written."""

    def test_all_keys_written(self, conn):
        agents = AgentsConfig(
            max_review_iterations=4,
            max_ci_retries=3,
            max_merge_conflict_retries=5,
            codex_reviewer_enabled=True,
            codex_reviewer_model="o3",
            codex_reviewer_timeout_minutes=20,
        )
        sync_agent_config_to_db(conn, agents)
        result = read_runtime_config(conn)
        assert set(result.keys()) == RUNTIME_CONFIG_KEYS
        assert result["max_review_iterations"] == 4
        assert result["max_ci_retries"] == 3
        assert result["max_merge_conflict_retries"] == 5
        assert result["codex_reviewer_enabled"] is True
        assert result["codex_reviewer_model"] == "o3"
        assert result["codex_reviewer_timeout_minutes"] == 20


class TestRuntimeConfigEmpty:
    """Empty table returns empty dict."""

    def test_empty_table(self, conn):
        result = read_runtime_config(conn)
        assert result == {}
