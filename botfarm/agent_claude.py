"""Claude agent adapter — wraps ``run_claude_streaming()`` behind :class:`AgentAdapter`."""

from __future__ import annotations

from pathlib import Path

from botfarm.agent import AgentResult, ContextFillCallback
from botfarm.worker_claude import ClaudeResult, check_claude_available, run_claude_streaming

_DEFAULT_MAX_TURNS = 200


class ClaudeAdapter:
    """Adapter that wraps :func:`run_claude_streaming` as an :class:`AgentAdapter`."""

    def __init__(self, *, auth_mode: str = "oauth") -> None:
        self._auth_mode = auth_mode

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
        claude_result = run_claude_streaming(
            prompt,
            cwd=cwd,
            max_turns=max_turns if max_turns is not None else _DEFAULT_MAX_TURNS,
            log_file=log_file,
            env=env,
            on_context_fill=on_context_fill,
            timeout=timeout,
            mcp_config=mcp_config,
            auth_mode=self._auth_mode,
            model=model,
            effort=effort,
            context_window=context_window,
        )
        return _claude_result_to_agent_result(claude_result)

    def check_available(self) -> tuple[bool, str]:
        return check_claude_available()

    def preflight_checks(self) -> list[tuple[str, bool, str]]:
        ok, msg = self.check_available()
        if ok:
            return [("available", True, f"OK — {msg}")]

        # Augment with recovery guidance when binary is missing.
        if "not found" in msg:
            local_bin = Path("~/.local/bin").expanduser()
            if (local_bin / "claude").exists():
                msg = (
                    "'claude' found at ~/.local/bin/claude but ~/.local/bin "
                    "is not in PATH. "
                    "Add it permanently: "
                    "echo 'export PATH=\"$HOME/.local/bin:$PATH\"' >> ~/.bashrc "
                    "&& source ~/.bashrc — "
                    "Or for nohup: PATH=$HOME/.local/bin:$PATH nohup botfarm run &"
                )
            else:
                msg = (
                    "'claude' not found on PATH and not present at "
                    "~/.local/bin/claude. "
                    "Install Claude Code: curl -fsSL https://claude.ai/install.sh "
                    "| bash — Then add ~/.local/bin to PATH if needed."
                )
        return [("available", False, msg)]


def create_adapter(*, auth_mode: str = "oauth", **_kwargs: object) -> ClaudeAdapter:
    """Entry-point factory for the Claude adapter."""
    return ClaudeAdapter(auth_mode=auth_mode)


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
