"""Tests for botfarm.agent — AgentResult dataclass and AgentAdapter protocol."""

from __future__ import annotations

from pathlib import Path

from botfarm.agent import AgentAdapter, AgentResult, ContextFillCallback


# ---------------------------------------------------------------------------
# AgentResult construction
# ---------------------------------------------------------------------------


class TestAgentResult:
    def test_minimal_construction(self):
        r = AgentResult(
            session_id="sess-1",
            num_turns=3,
            duration_seconds=12.5,
            result_text="done",
        )
        assert r.session_id == "sess-1"
        assert r.num_turns == 3
        assert r.duration_seconds == 12.5
        assert r.result_text == "done"
        assert r.is_error is False
        assert r.input_tokens == 0
        assert r.output_tokens == 0
        assert r.cost_usd == 0.0
        assert r.context_fill_pct is None
        assert r.extra == {}

    def test_full_construction(self):
        extra = {"model_usage_json": "{}", "exit_subtype": "tool_use"}
        r = AgentResult(
            session_id="sess-2",
            num_turns=10,
            duration_seconds=60.0,
            result_text="output text",
            is_error=True,
            input_tokens=5000,
            output_tokens=2000,
            cost_usd=0.12,
            context_fill_pct=87.5,
            extra=extra,
        )
        assert r.is_error is True
        assert r.input_tokens == 5000
        assert r.output_tokens == 2000
        assert r.cost_usd == 0.12
        assert r.context_fill_pct == 87.5
        assert r.extra is extra

    def test_extra_default_is_independent(self):
        """Each instance gets its own empty dict for extra."""
        r1 = AgentResult(
            session_id="a", num_turns=1, duration_seconds=1.0, result_text=""
        )
        r2 = AgentResult(
            session_id="b", num_turns=1, duration_seconds=1.0, result_text=""
        )
        r1.extra["key"] = "val"
        assert "key" not in r2.extra


# ---------------------------------------------------------------------------
# AgentAdapter protocol conformance
# ---------------------------------------------------------------------------


class _DummyAdapter:
    """Minimal concrete implementation of the AgentAdapter protocol."""

    @property
    def name(self) -> str:
        return "dummy"

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
        return AgentResult(
            session_id="dummy-sess",
            num_turns=1,
            duration_seconds=0.1,
            result_text="ok",
        )

    def calculate_cost(self, result: AgentResult) -> float:
        return result.cost_usd

    def check_available(self) -> tuple[bool, str]:
        return (True, "")

    def preflight_checks(self) -> list[tuple[str, bool, str]]:
        return [("available", True, "OK")]


class _IncompleteAdapter:
    """Missing required methods — should NOT satisfy the protocol."""

    @property
    def name(self) -> str:
        return "broken"


class TestAgentAdapterProtocol:
    def test_dummy_adapter_is_instance(self):
        adapter = _DummyAdapter()
        assert isinstance(adapter, AgentAdapter)

    def test_incomplete_adapter_is_not_instance(self):
        adapter = _IncompleteAdapter()
        assert not isinstance(adapter, AgentAdapter)

    def test_dummy_adapter_run(self, tmp_path: Path):
        adapter = _DummyAdapter()
        result = adapter.run("test prompt", cwd=tmp_path)
        assert isinstance(result, AgentResult)
        assert result.session_id == "dummy-sess"
        assert result.result_text == "ok"

    def test_dummy_adapter_check_available(self):
        adapter = _DummyAdapter()
        available, msg = adapter.check_available()
        assert available is True
        assert msg == ""

    def test_adapter_properties(self):
        adapter = _DummyAdapter()
        assert adapter.name == "dummy"
        assert adapter.supports_context_fill is True
        assert adapter.supports_max_turns is True
        assert adapter.supports_model_override is False


# ---------------------------------------------------------------------------
# ContextFillCallback type alias
# ---------------------------------------------------------------------------


class TestContextFillCallback:
    def test_callable_matches_signature(self):
        calls: list[tuple[int, float]] = []

        def cb(turn: int, fill: float) -> None:
            calls.append((turn, fill))

        # Verify the callback type works as expected
        fn: ContextFillCallback = cb
        fn(1, 50.0)
        fn(2, 75.5)
        assert calls == [(1, 50.0), (2, 75.5)]
