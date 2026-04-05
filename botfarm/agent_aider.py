"""Aider agent adapter — wraps ``run_aider()`` behind :class:`AgentAdapter`.

Reference implementation showing how to add new agent backends to botfarm.
Aider supports many models (including open-source via OpenRouter), making it
strategic for fully self-hostable deployments.
"""

from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path

from botfarm.agent import (
    AdapterConfigSchema,
    AgentResult,
    ConfigFieldSchema,
    ContextFillCallback,
)
from botfarm.aider import AiderResult, check_aider_available, run_aider

logger = logging.getLogger(__name__)


class AiderAdapter:
    """Adapter that wraps :func:`run_aider` as an :class:`AgentAdapter`.

    Parameters
    ----------
    model:
        Optional default model name passed to Aider via ``--model``.
    """

    def __init__(self, *, model: str | None = None) -> None:
        self._model = model

    @property
    def name(self) -> str:
        return "aider"

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
        aider_result = run_aider(
            prompt,
            cwd=cwd,
            model=effective_model,
            log_file=log_file,
            env=env,
            timeout=timeout,
        )
        return _aider_result_to_agent_result(aider_result, effective_model)

    @classmethod
    def config_schema(cls) -> AdapterConfigSchema:
        return AdapterConfigSchema(
            description="Aider agent — supports many models via OpenAI, Anthropic, OpenRouter",
            fields=[
                ConfigFieldSchema("enabled", bool, default=False, description="Enable this adapter"),
                ConfigFieldSchema("model", str, default="", description='Model name (e.g. "gpt-4o", "claude-3-5-sonnet")'),
                ConfigFieldSchema("timeout_minutes", int, default=None, description="Per-stage timeout in minutes"),
                ConfigFieldSchema("reasoning_effort", str, default="", description="Reasoning effort level"),
                ConfigFieldSchema("skip_on_reiteration", bool, default=True, description="Skip on review iterations 2+"),
            ],
            required_env_vars=[],
        )

    def calculate_cost(self, result: AgentResult) -> float:
        # Aider reports session cost in its output; use that directly.
        return result.cost_usd

    def check_available(self) -> tuple[bool, str]:
        return check_aider_available()

    def preflight_checks(self) -> list[tuple[str, bool, str]]:
        results: list[tuple[str, bool, str]] = []

        ok, msg = self.check_available()
        if ok:
            results.append(("available", True, f"OK — {msg}"))
        else:
            results.append((
                "available",
                False,
                f"{msg} — install with: pip install aider-chat",
            ))

        # Aider needs at least one provider API key.
        has_openai = bool(os.environ.get("OPENAI_API_KEY"))
        has_anthropic = bool(os.environ.get("ANTHROPIC_API_KEY"))
        has_openrouter = bool(os.environ.get("OPENROUTER_API_KEY"))
        if not (has_openai or has_anthropic or has_openrouter):
            results.append((
                "auth",
                False,
                "No API key found — Aider requires at least one of: "
                "OPENAI_API_KEY, ANTHROPIC_API_KEY, OPENROUTER_API_KEY",
            ))
        else:
            providers = []
            if has_openai:
                providers.append("OpenAI")
            if has_anthropic:
                providers.append("Anthropic")
            if has_openrouter:
                providers.append("OpenRouter")
            results.append((
                "auth",
                True,
                f"OK — API key(s) found for: {', '.join(providers)}",
            ))

        return results


def create_adapter(*, model: str | None = None, **_kwargs: object) -> AiderAdapter:
    """Entry-point factory for the Aider adapter."""
    return AiderAdapter(model=model or None)


create_adapter.config_schema = AiderAdapter.config_schema  # type: ignore[attr-defined]


def _aider_result_to_agent_result(
    ar: AiderResult, model: str | None
) -> AgentResult:
    """Normalize an :class:`AiderResult` into an :class:`AgentResult`.

    Aider reports session cost in its output, so ``cost_usd`` is set
    directly from the parsed value.
    """
    return AgentResult(
        session_id=uuid.uuid4().hex[:12],
        num_turns=1,  # Aider processes the full message in one pass
        duration_seconds=ar.duration_seconds,
        result_text=ar.result_text,
        is_error=ar.is_error,
        input_tokens=ar.input_tokens,
        output_tokens=ar.output_tokens,
        cost_usd=ar.cost_usd,
        context_fill_pct=None,
        extra={
            "model": model or ar.model or "",
        },
    )
