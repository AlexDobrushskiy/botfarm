"""Claude agent adapter — wraps ``run_claude_streaming()`` behind :class:`AgentAdapter`."""

from __future__ import annotations

import subprocess
from pathlib import Path

from botfarm.agent import AgentResult, ContextFillCallback
from botfarm.worker_claude import ClaudeResult, run_claude_streaming


class ClaudeAdapter:
    """Adapter that wraps :func:`run_claude_streaming` as an :class:`AgentAdapter`."""

    @property
    def name(self) -> str:
        return "claude"

    @property
    def supports_context_fill(self) -> bool:
        return True

    @property
    def supports_max_turns(self) -> bool:
        return True

    @property
    def supports_model_override(self) -> bool:
        return False

    def run(
        self,
        prompt: str,
        *,
        cwd: Path,
        max_turns: int | None = None,
        model: str | None = None,
        log_file: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        on_context_fill: ContextFillCallback | None = None,
    ) -> AgentResult:
        claude_result = run_claude_streaming(
            prompt,
            cwd=cwd,
            max_turns=max_turns or 200,
            log_file=log_file,
            env=env,
            on_context_fill=on_context_fill,
            timeout=timeout,
        )
        return _claude_result_to_agent_result(claude_result)

    def check_available(self) -> tuple[bool, str]:
        try:
            result = subprocess.run(
                ["claude", "--version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                version = result.stdout.strip()
                return True, f"claude {version}"
            return False, f"claude --version exited with code {result.returncode}"
        except FileNotFoundError:
            return False, "claude binary not found on PATH"
        except subprocess.TimeoutExpired:
            return False, "claude --version timed out"
        except OSError as exc:
            return False, f"claude check failed: {exc}"


def _claude_result_to_agent_result(cr: ClaudeResult) -> AgentResult:
    """Normalize a :class:`ClaudeResult` into an :class:`AgentResult`."""
    return AgentResult(
        session_id=cr.session_id,
        num_turns=cr.num_turns,
        duration_seconds=cr.duration_seconds,
        result_text=cr.result_text,
        is_error=cr.is_error,
        input_tokens=cr.input_tokens,
        output_tokens=cr.output_tokens,
        cost_usd=cr.total_cost_usd,
        context_fill_pct=cr.context_fill_pct,
        extra={
            "exit_subtype": cr.exit_subtype,
            "model_usage_json": cr.model_usage_json,
            "cache_creation_input_tokens": cr.cache_creation_input_tokens,
            "cache_read_input_tokens": cr.cache_read_input_tokens,
        },
    )
