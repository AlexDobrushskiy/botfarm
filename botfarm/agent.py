"""Foundation types for the agent abstraction layer.

Defines :class:`AgentResult` (unified result type), :class:`AgentAdapter`
(protocol for agent backends), :data:`AdapterRegistry` (mapping of executor
type names to adapter instances), and the :data:`ContextFillCallback` type alias.
"""

from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

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
        effort: str | None = None,
        context_window: int | None = None,
        log_file: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        on_context_fill: ContextFillCallback | None = None,
        mcp_config: str | None = None,
    ) -> AgentResult:
        """Execute the agent with the given prompt and return a unified result."""
        ...

    def calculate_cost(self, result: AgentResult) -> float:
        """Calculate cost in USD from an :class:`AgentResult`.

        Each adapter owns its pricing logic.  Token-based adapters should
        use :func:`calculate_cost_from_table`; adapters whose CLI already
        reports cost can simply return ``result.cost_usd``.
        """
        ...

    def check_available(self) -> tuple[bool, str]:
        """Preflight availability check.

        Returns (available, message) where message explains why the
        adapter is unavailable when available is False.
        """
        ...

    def preflight_checks(self) -> list[tuple[str, bool, str]]:
        """Return adapter-specific preflight check results.

        Each tuple is ``(check_name, passed, message)`` where *check_name*
        is a short suffix like ``"available"`` or ``"auth"``.

        Implementations should delegate to :meth:`check_available` for the
        binary probe and add any adapter-specific checks (auth, tooling)
        on top.
        """
        ...


def calculate_cost_from_table(
    pricing: dict[str, dict[str, float]],
    *,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int = 0,
) -> float | None:
    """Calculate USD cost from a pricing table and token counts.

    *pricing* maps model names to dicts with ``"input"``, ``"output"``,
    and optionally ``"cached_input"`` keys (prices per 1 M tokens).

    Returns ``None`` if *model* is not in the pricing table.
    """
    prices = pricing.get(model)
    if prices is None:
        return None
    non_cached = input_tokens - cached_input_tokens
    return (
        non_cached * prices["input"] / 1_000_000
        + cached_input_tokens * prices.get("cached_input", prices["input"]) / 1_000_000
        + output_tokens * prices["output"] / 1_000_000
    )


# Type alias: maps executor_type names (e.g. "claude", "codex") to adapter instances.
AdapterRegistry = dict[str, AgentAdapter]


def build_adapter_registry(
    adapter_configs: dict[str, dict[str, Any]] | None = None,
    *,
    # Legacy kwargs — prefer adapter_configs instead.
    codex_model: str | None = None,
    codex_reasoning_effort: str | None = None,
    auth_mode: str = "oauth",
) -> AdapterRegistry:
    """Build the adapter registry via setuptools entry-point discovery.

    Discovers adapters registered under the ``botfarm.adapters`` entry-point
    group.  Each entry point must resolve to a factory callable that accepts
    ``**kwargs`` and returns an :class:`AgentAdapter` instance.

    Parameters
    ----------
    adapter_configs:
        Optional mapping of adapter names to config keyword-argument dicts.
        Each dict is passed as keyword arguments to the corresponding
        adapter factory.
    auth_mode:
        Authentication mode, forwarded to all adapter factories.
    codex_model, codex_reasoning_effort:
        Legacy keyword arguments.  Use ``adapter_configs`` instead.
    """
    eps = importlib.metadata.entry_points(group="botfarm.adapters")

    # Merge legacy kwargs into adapter_configs for backward compatibility.
    configs: dict[str, dict[str, Any]] = dict(adapter_configs or {})
    if "codex" not in configs and (codex_model is not None or codex_reasoning_effort is not None):
        configs["codex"] = {
            k: v
            for k, v in [("model", codex_model), ("reasoning_effort", codex_reasoning_effort)]
            if v is not None
        }

    # Global kwargs forwarded to every factory.
    global_kwargs: dict[str, Any] = {"auth_mode": auth_mode}

    registry: AdapterRegistry = {}
    for ep in eps:
        try:
            factory = ep.load()
        except Exception:
            logger.warning(
                "Failed to load adapter entry point %r (%s)",
                ep.name, ep.value, exc_info=True,
            )
            continue
        kwargs = {**global_kwargs, **configs.get(ep.name, {})}
        try:
            adapter = factory(**kwargs)
        except Exception:
            logger.warning(
                "Failed to create adapter %r from entry point %s",
                ep.name, ep.value, exc_info=True,
            )
            continue
        registry[ep.name] = adapter

    return registry
