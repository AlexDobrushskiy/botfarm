"""Model list fetching and caching via the Anthropic Models API."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)

ANTHROPIC_MODELS_URL = "https://api.anthropic.com/v1/models"
ANTHROPIC_API_VERSION = "2023-06-01"

# Seed data used when the API is unavailable (e.g. oauth-only auth).
SEED_MODELS: list[dict[str, Any]] = [
    {
        "id": "claude-opus-4-6",
        "display_name": "Claude Opus 4.6",
        "max_input_tokens": 1_000_000,
        "max_output_tokens": 32_000,
        "supported_efforts": '["low","medium","high","max"]',
        "executor_type": "claude",
        "is_alias": 0,
    },
    {
        "id": "claude-sonnet-4-6",
        "display_name": "Claude Sonnet 4.6",
        "max_input_tokens": 200_000,
        "max_output_tokens": 16_000,
        "supported_efforts": '["low","medium","high","max"]',
        "executor_type": "claude",
        "is_alias": 0,
    },
    {
        "id": "claude-haiku-4-5-20251001",
        "display_name": "Claude Haiku 4.5",
        "max_input_tokens": 200_000,
        "max_output_tokens": 8_192,
        "supported_efforts": '["low","medium","high","max"]',
        "executor_type": "claude",
        "is_alias": 0,
    },
]


@dataclass
class CachedModel:
    """A model record from the available_models table."""

    id: str
    display_name: str
    max_input_tokens: int
    max_output_tokens: int
    supported_efforts: list[str] | None
    executor_type: str
    is_alias: bool
    fetched_at: str


def _row_to_model(row: sqlite3.Row) -> CachedModel:
    """Convert a DB row to a CachedModel."""
    efforts_raw = row["supported_efforts"]
    efforts = json.loads(efforts_raw) if efforts_raw else None
    return CachedModel(
        id=row["id"],
        display_name=row["display_name"],
        max_input_tokens=row["max_input_tokens"],
        max_output_tokens=row["max_output_tokens"],
        supported_efforts=efforts,
        executor_type=row["executor_type"],
        is_alias=bool(row["is_alias"]),
        fetched_at=row["fetched_at"],
    )


def get_cached_models(
    conn: sqlite3.Connection, executor_type: str = "claude"
) -> list[CachedModel]:
    """Read cached models from the DB, filtered by executor_type."""
    rows = conn.execute(
        "SELECT * FROM available_models WHERE executor_type = ? ORDER BY id",
        (executor_type,),
    ).fetchall()
    return [_row_to_model(r) for r in rows]


def _fetch_models_from_api(api_key: str) -> list[dict[str, Any]]:
    """Call the Anthropic Models API and return parsed model dicts."""
    all_models: list[dict[str, Any]] = []
    has_more = True
    after_id: str | None = None

    while has_more:
        params: dict[str, str] = {"limit": "100"}
        if after_id:
            params["after_id"] = after_id

        resp = httpx.get(
            ANTHROPIC_MODELS_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_API_VERSION,
            },
            params=params,
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()

        for m in data.get("data", []):
            efforts = None
            caps = m.get("capabilities", {})
            if caps.get("extended_thinking"):
                efforts = json.dumps(["low", "medium", "high", "max"])

            all_models.append({
                "id": m["id"],
                "display_name": m.get("display_name", m["id"]),
                "max_input_tokens": m.get("max_input_tokens", 0),
                "max_output_tokens": m.get("max_output_tokens", 0),
                "supported_efforts": efforts,
                "executor_type": "claude",
                "is_alias": 0,
            })

        has_more = data.get("has_more", False)
        if has_more and all_models:
            after_id = all_models[-1]["id"]

    return all_models


def _upsert_models(conn: sqlite3.Connection, models: list[dict[str, Any]]) -> None:
    """Insert or replace model rows in the DB."""
    for m in models:
        conn.execute(
            "INSERT OR REPLACE INTO available_models "
            "(id, display_name, max_input_tokens, max_output_tokens, "
            "supported_efforts, executor_type, is_alias, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            (
                m["id"],
                m["display_name"],
                m["max_input_tokens"],
                m["max_output_tokens"],
                m["supported_efforts"],
                m["executor_type"],
                m["is_alias"],
            ),
        )
    conn.commit()


def refresh_models(conn: sqlite3.Connection, api_key: str) -> list[CachedModel]:
    """Fetch the model list from the Anthropic API and upsert into the DB.

    Returns the updated list of cached models.
    """
    raw_models = _fetch_models_from_api(api_key)
    _upsert_models(conn, raw_models)
    return get_cached_models(conn)


def ensure_seed_data(conn: sqlite3.Connection) -> None:
    """Insert seed models if the available_models table is empty."""
    count = conn.execute("SELECT COUNT(*) AS cnt FROM available_models").fetchone()["cnt"]
    if count > 0:
        return
    _upsert_models(conn, SEED_MODELS)
