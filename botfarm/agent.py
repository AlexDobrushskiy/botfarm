"""Foundation types for the agent abstraction layer.

Defines :class:`AgentResult` (unified result type), :class:`AgentAdapter`
(protocol for agent backends), and the :data:`ContextFillCallback` type alias.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

# Type alias for the per-turn context fill callback.
# Called with (turn_number, fill_percentage).
ContextFillCallback = Callable[[int, float], None]


@dataclass
class AgentResult:
    """Unified result type that all agent adapters normalize their output into."""

    session_id: str
    num_turns: int
    duration_seconds: float
    result_text: str
    is_error: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    context_fill_pct: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class AgentAdapter(Protocol):
    """Protocol defining the interface for agent backends (e.g. Claude, Codex)."""

    @property
    def name(self) -> str:
        """Identifier matching ``executor_type`` in stage_templates."""
        ...

    @property
    def supports_context_fill(self) -> bool:
        """Whether adapter supports per-turn context fill callbacks."""
        ...

    @property
    def supports_max_turns(self) -> bool:
        """Whether the agent CLI supports a max-turns parameter."""
        ...

    @property
    def supports_model_override(self) -> bool:
        """Whether model can be overridden at runtime."""
        ...

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
        """Execute the agent with the given prompt and return a unified result."""
        ...

    def check_available(self) -> tuple[bool, str]:
        """Preflight availability check.

        Returns (available, message) where message explains why the
        adapter is unavailable when available is False.
        """
        ...
