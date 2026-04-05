"""Codex agent adapter — wraps ``run_codex_streaming()`` behind :class:`AgentAdapter`."""

from __future__ import annotations

from pathlib import Path

from botfarm.agent import AgentResult, ContextFillCallback
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

    def check_available(self) -> tuple[bool, str]:
        return check_codex_available()


def create_adapter(
    *, model: str | None = None, reasoning_effort: str | None = None, **_kwargs: object
) -> CodexAdapter:
    """Entry-point factory for the Codex adapter."""
    return CodexAdapter(model=model or None, reasoning_effort=reasoning_effort or None)


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
