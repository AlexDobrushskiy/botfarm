"""Codex agent adapter — wraps ``run_codex_streaming()`` behind :class:`AgentAdapter`."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from botfarm.agent import (
    AdapterConfigSchema,
    AgentResult,
    ConfigFieldSchema,
    ContextFillCallback,
    calculate_cost_from_table,
)
from botfarm.codex import (
    CodexResult,
    check_codex_available,
    run_codex_streaming,
)

logger = logging.getLogger(__name__)

# OpenAI model pricing (per 1M tokens). Pay-per-token API list prices —
# subscription users pay differently, but list prices enable eval cost comparisons.
# Sources: pricepertoken.com/openai (Apr 2026), openai.com/api/pricing.
OPENAI_PRICING: dict[str, dict[str, float]] = {
    # Current frontier agentic coding models (Codex CLI >= 0.120)
    "gpt-5.4": {"input": 2.50, "cached_input": 0.25, "output": 15.00},
    "gpt-5.4-mini": {"input": 0.75, "cached_input": 0.075, "output": 4.50},
    "gpt-5.3-codex": {"input": 1.75, "cached_input": 0.175, "output": 14.00},
    "gpt-5.2": {"input": 0.875, "cached_input": 0.175, "output": 7.00},
    # Legacy models (retained for backward compat / older configs)
    "o3": {"input": 2.00, "cached_input": 0.50, "output": 8.00},
    "o4-mini": {"input": 1.10, "cached_input": 0.275, "output": 4.40},
    "gpt-4o": {"input": 2.50, "cached_input": 1.25, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "cached_input": 0.075, "output": 0.60},
}

DEFAULT_CODEX_MODEL = "o4-mini"


class CodexAdapter:
    """Adapter that wraps :func:`run_codex_streaming` as an :class:`AgentAdapter`.

    Parameters
    ----------
    model:
        Optional model name passed to Codex via ``-m``.
    reasoning_effort:
        Optional reasoning effort level for Codex.
    """

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> None:
        self._model = model
        self._reasoning_effort = reasoning_effort

    @property
    def name(self) -> str:
        return "codex"

    @property
    def supports_context_fill(self) -> bool:
        return False

    @property
    def supports_max_turns(self) -> bool:
        return False

    @property
    def supports_model_override(self) -> bool:
        return True

    def run(
        self,
        prompt: str,
        *,
        cwd: Path,
        max_turns: int | None = None,
        model: str | None = None,
        effort: str | None = None,
        context_window: int | None = None,
        thinking_mode: str | None = None,
        log_file: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        on_context_fill: ContextFillCallback | None = None,
        mcp_config: str | None = None,
    ) -> AgentResult:
        effective_model = model or self._model
        effective_effort = effort or self._reasoning_effort
        codex_result = run_codex_streaming(
            prompt,
            cwd=cwd,
            model=effective_model,
            reasoning_effort=effective_effort,
            log_file=log_file,
            env=env,
            timeout=timeout,
        )
        return _codex_result_to_agent_result(codex_result, effective_model)

    @classmethod
    def config_schema(cls) -> AdapterConfigSchema:
        return AdapterConfigSchema(
            description="OpenAI Codex agent via Codex CLI",
            fields=[
                ConfigFieldSchema("enabled", bool, default=False, description="Enable this adapter"),
                ConfigFieldSchema("model", str, default="", description='Model name (e.g. "gpt-5.4", "gpt-5.4-mini")'),
                ConfigFieldSchema("timeout_minutes", int, default=15, description="Per-stage timeout in minutes"),
                ConfigFieldSchema("reasoning_effort", str, default="medium", description="low, medium, high, xhigh"),
                ConfigFieldSchema("skip_on_reiteration", bool, default=True, description="Skip on review iterations 2+"),
            ],
            required_env_vars=[],
        )

    def calculate_cost(self, result: AgentResult) -> float:
        model = result.extra.get("model", "") or self._model or DEFAULT_CODEX_MODEL
        cost = calculate_cost_from_table(
            OPENAI_PRICING,
            model=model,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cached_input_tokens=result.extra.get("cache_read_input_tokens", 0),
        )
        if cost is None:
            logger.warning(
                "Unknown OpenAI model %r — skipping cost calculation", model
            )
            return 0.0
        return cost

    def check_available(self) -> tuple[bool, str]:
        return check_codex_available()

    def preflight_checks(self) -> list[tuple[str, bool, str]]:
        results: list[tuple[str, bool, str]] = []

        ok, msg = self.check_available()
        if ok:
            results.append(("available", True, f"OK — {msg}"))
        else:
            results.append((
                "available",
                False,
                f"{msg} — install Codex or disable the codex adapter",
            ))

        has_api_key = bool(os.environ.get("OPENAI_API_KEY"))
        has_auth_file = Path("~/.codex/auth.json").expanduser().exists()
        if not has_api_key and not has_auth_file:
            results.append((
                "auth",
                False,
                "OPENAI_API_KEY is not set and ~/.codex/auth.json not found — "
                "Codex requires one of these for authentication",
            ))
        else:
            results.append(("auth", True, "OK — Codex authentication available"))

        return results


def create_adapter(
    *, model: str | None = None, reasoning_effort: str | None = None, **_kwargs: object
) -> CodexAdapter:
    """Entry-point factory for the Codex adapter."""
    return CodexAdapter(model=model or None, reasoning_effort=reasoning_effort or None)


create_adapter.config_schema = CodexAdapter.config_schema  # type: ignore[attr-defined]


def _codex_result_to_agent_result(
    cr: CodexResult, model: str | None
) -> AgentResult:
    """Normalize a :class:`CodexResult` into an :class:`AgentResult`.

    Cost is left at 0.0 here — :meth:`CodexAdapter.calculate_cost` is the
    single authoritative pricing path, called from ``_record_stage_run``.
    """
    effective_model = model or cr.model or ""
    return AgentResult(
        session_id=cr.thread_id,
        num_turns=cr.num_turns,
        duration_seconds=cr.duration_seconds,
        result_text=cr.result_text,
        is_error=cr.is_error,
        input_tokens=cr.input_tokens,
        output_tokens=cr.output_tokens,
        cost_usd=0.0,
        context_fill_pct=None,
        extra={
            "thread_id": cr.thread_id,
            "cache_read_input_tokens": cr.cached_input_tokens,
            "cache_creation_input_tokens": 0,
            "model": effective_model,
        },
    )
