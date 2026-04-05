"""Codex agent adapter — wraps ``run_codex_streaming()`` behind :class:`AgentAdapter`."""

from __future__ import annotations

import os
from pathlib import Path

from botfarm.agent import (
    AdapterConfigSchema,
    AgentResult,
    ConfigFieldSchema,
    ContextFillCallback,
)
from botfarm.codex import (
    CodexResult,
    calculate_codex_cost,
    check_codex_available,
    run_codex_streaming,
)


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
        log_file: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        on_context_fill: ContextFillCallback | None = None,
        mcp_config: str | None = None,
    ) -> AgentResult:
        effective_model = model or self._model
        codex_result = run_codex_streaming(
            prompt,
            cwd=cwd,
            model=effective_model,
            reasoning_effort=self._reasoning_effort,
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
                ConfigFieldSchema("model", str, default="", description='Model name (e.g. "o3", "o4-mini")'),
                ConfigFieldSchema("timeout_minutes", int, default=15, description="Per-stage timeout in minutes"),
                ConfigFieldSchema("reasoning_effort", str, default="medium", description="none, low, medium, high, xhigh"),
                ConfigFieldSchema("skip_on_reiteration", bool, default=True, description="Skip on review iterations 2+"),
            ],
            required_env_vars=[
                ("OPENAI_API_KEY", "OpenAI API key (alternative: ~/.codex/auth.json)"),
            ],
        )

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
    """Normalize a :class:`CodexResult` into an :class:`AgentResult`."""
    cost = calculate_codex_cost(
        model=model or cr.model or "",
        input_tokens=cr.input_tokens,
        output_tokens=cr.output_tokens,
        cached_input_tokens=cr.cached_input_tokens,
    )
    return AgentResult(
        session_id=cr.thread_id,
        num_turns=cr.num_turns,
        duration_seconds=cr.duration_seconds,
        result_text=cr.result_text,
        is_error=cr.is_error,
        input_tokens=cr.input_tokens,
        output_tokens=cr.output_tokens,
        cost_usd=cost if cost is not None else 0.0,
        context_fill_pct=None,
        extra={
            "thread_id": cr.thread_id,
            "cache_read_input_tokens": cr.cached_input_tokens,
            "cache_creation_input_tokens": 0,
            "model": model or cr.model or "",
        },
    )
