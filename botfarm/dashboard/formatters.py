"""NDJSON and display formatting helpers — pure functions, no app dependencies."""

from __future__ import annotations

import json


def _summarize_tool_input(tool_name: str, tool_input: dict) -> str:
    """Produce a short summary of tool input for display."""
    if tool_name in ("Read", "Glob"):
        return tool_input.get("file_path") or tool_input.get("pattern", "")
    if tool_name == "Edit":
        fp = tool_input.get("file_path", "")
        return fp
    if tool_name == "Write":
        return tool_input.get("file_path", "")
    if tool_name == "Bash":
        cmd = tool_input.get("command", "")
        return cmd[:80] if cmd else ""
    if tool_name == "Grep":
        return tool_input.get("pattern", "")
    # Generic: show first key=value
    for k, v in tool_input.items():
        sv = str(v)[:60]
        return f"{k}={sv}"
    return ""


def _format_assistant(event: dict) -> tuple[str, str]:
    """Format an ``assistant`` NDJSON event.

    Extracts text content and lists tool_use calls.
    """
    message = event.get("message") or {}
    content_blocks = message.get("content") or []

    parts: list[str] = []
    tool_lines: list[str] = []

    for block in content_blocks:
        block_type = block.get("type", "")
        if block_type == "text":
            text = block.get("text", "").strip()
            if text:
                parts.append(text)
        elif block_type == "tool_use":
            tool_name = block.get("name", "unknown")
            tool_input = block.get("input") or {}
            summary = _summarize_tool_input(tool_name, tool_input)
            tool_lines.append(f"  -> {tool_name}({summary})")

    lines: list[str] = []
    if parts:
        lines.append("\n".join(parts))
    if tool_lines:
        lines.extend(tool_lines)

    if not lines:
        return ("assistant", "[assistant turn]")

    # When only tool_use blocks are present (no text), categorise as "tool_use"
    # so the dashboard can apply distinct purple styling.
    event_type = "tool_use" if (tool_lines and not parts) else "assistant"
    return (event_type, "\n".join(lines))


def _format_user(event: dict) -> tuple[str, str]:
    """Format a ``user`` NDJSON event (tool results)."""
    message = event.get("message") or {}
    content_blocks = message.get("content") or []

    results: list[str] = []
    for block in content_blocks:
        block_type = block.get("type", "")
        if block_type == "tool_result":
            is_error = block.get("is_error", False)
            status = "ERROR" if is_error else "ok"
            # Content may be a string or list of content blocks
            content = block.get("content", "")
            snippet = ""
            if isinstance(content, str):
                snippet = content[:120]
            elif isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        snippet = item.get("text", "")[:120]
                        break
            if snippet:
                results.append(f"  [{status}] {snippet}")
            else:
                results.append(f"  [{status}]")

    if not results:
        return ("tool_result", "[tool results]")
    return ("tool_result", "\n".join(results))


def _format_result(event: dict) -> tuple[str, str]:
    """Format a ``result`` NDJSON event (stage completion)."""
    num_turns = event.get("num_turns", 0)
    duration_ms = event.get("duration_ms", 0)
    duration_s = duration_ms / 1000.0 if duration_ms else 0
    subtype = event.get("subtype", "")
    is_error = event.get("is_error", False)
    result_text = (event.get("result") or "")[:200]

    parts = [f"Completed in {num_turns} turns ({duration_s:.1f}s)"]
    if subtype:
        parts[0] += f" [{subtype}]"
    if is_error:
        parts[0] += " [ERROR]"
    if result_text:
        parts.append(result_text)
    return ("result", "\n".join(parts))


# ---------------------------------------------------------------------------
# NDJSON line formatting for SSE log streaming
# ---------------------------------------------------------------------------


def format_ndjson_line(raw_line: str) -> tuple[str, str]:
    """Parse a raw NDJSON line and return ``(event_type, formatted_text)``.

    *event_type* is one of: ``assistant``, ``tool_use``, ``tool_result``,
    ``result``, ``system``, or ``log`` (fallback for non-JSON lines).

    *formatted_text* is a human-readable summary of the event suitable for
    display in a terminal-style log viewer.
    """
    stripped = raw_line.strip()
    if not stripped:
        return ("log", "")

    try:
        event = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        # Not JSON — pass through as-is
        return ("log", stripped)

    if not isinstance(event, dict):
        return ("log", stripped)

    msg_type = event.get("type", "")

    if msg_type == "assistant":
        return _format_assistant(event)
    if msg_type == "user":
        return _format_user(event)
    if msg_type == "result":
        return _format_result(event)

    # Other NDJSON event types (e.g. system messages) — show type as prefix
    if msg_type:
        return ("system", f"[{msg_type}]")

    return ("log", stripped)


def format_codex_ndjson_line(raw_line: str) -> tuple[str, str]:
    """Parse a raw Codex JSONL line and return ``(event_type, formatted_text)``.

    Handles Codex Responses API event types:
    ``thread.started``, ``turn.started``, ``item.completed``,
    ``turn.completed``.

    For ``item.completed`` events, ``agent_message`` text is shown
    prominently, ``reasoning`` is dimmed, and ``shell_command`` /
    ``shell_result`` are shown in code block style.
    """
    stripped = raw_line.strip()
    if not stripped:
        return ("log", "")

    try:
        event = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return ("log", stripped)

    if not isinstance(event, dict):
        return ("log", stripped)

    event_type = event.get("type", "")

    if event_type == "thread.started":
        return ("system", "[Codex thread started]")

    if event_type == "turn.started":
        return ("system", "[Codex turn started]")

    if event_type == "turn.completed":
        status = event.get("status", "")
        return ("result", f"[Codex turn completed: {status}]" if status else "[Codex turn completed]")

    if event_type == "item.completed":
        item = event.get("item") or {}
        item_type = item.get("type", "")

        if item_type == "agent_message":
            text = ""
            for block in item.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text += block.get("text", "")
            return ("assistant", text.strip() if text.strip() else "[agent message]")

        if item_type == "reasoning":
            text = ""
            for block in item.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text += block.get("text", "")
            return ("system", text.strip() if text.strip() else "[reasoning]")

        if item_type == "shell_command":
            cmd = item.get("command", "")
            return ("tool_use", f"$ {cmd}" if cmd else "[shell command]")

        if item_type == "shell_result":
            output = item.get("output", "")
            snippet = output[:500] if output else ""
            return ("tool_result", snippet if snippet else "[shell result]")

        # Other item types — show type
        return ("system", f"[{item_type}]" if item_type else "[item]")

    # Unknown event type — show raw type
    if event_type:
        return ("system", f"[{event_type}]")

    return ("log", stripped)


def review_display_status(exit_subtype: str | None) -> str:
    """Map a review stage exit_subtype to a human-readable display status."""
    exit_sub = (exit_subtype or "").lower()
    if exit_sub in ("approved", "changes_requested"):
        return exit_sub.upper()
    if exit_sub == "skipped":
        return "Skipped"
    if exit_sub in ("failed", "error"):
        return "Failed"
    return "In Progress"


def build_pipeline_state(
    stage_runs: list[dict], task_status: str | None,
    stages: list[str] | None = None,
) -> list[dict]:
    """Aggregate stage runs into per-stage pipeline state for the stepper.

    Returns a list of dicts (one per canonical stage) with keys:
        name, status, iteration_count, has_limit_restart, codex_review
    """
    if stages is None:
        from botfarm.worker import STAGES
        stages = STAGES
    # Collect info per stage, excluding codex_review from pipeline stages
    stage_info: dict[str, dict] = {}
    codex_runs: list[dict] = []
    for run in stage_runs:
        name = run["stage"]
        if name == "codex_review":
            codex_runs.append(run)
            continue
        if name not in stage_info:
            stage_info[name] = {
                "count": 0,
                "has_limit_restart": False,
            }
        info = stage_info[name]
        info["count"] += 1
        if run.get("was_limit_restart"):
            info["has_limit_restart"] = True

    # Summarise Codex review runs for attachment to the review stage
    codex_summary = None
    if codex_runs:
        last = codex_runs[-1]
        codex_status = review_display_status(last.get("exit_subtype"))
        codex_summary = {"status": codex_status, "count": len(codex_runs)}

    # Find the last stage that has runs (by canonical order)
    last_run_idx = -1
    for i, stage_name in enumerate(stages):
        if stage_name in stage_info:
            last_run_idx = i

    result = []
    for i, stage_name in enumerate(stages):
        info = stage_info.get(stage_name)
        if info is None:
            # No runs for this stage — but may have been skipped
            status = "completed" if i < last_run_idx else "pending"
        elif i < last_run_idx:
            # A later stage has runs, so this one completed
            status = "completed"
        elif i == last_run_idx:
            # This is the last stage with runs — depends on task status
            if task_status == "completed":
                status = "completed"
            elif task_status == "failed":
                status = "failed"
            else:
                status = "active"
        else:
            status = "pending"

        entry: dict = {
            "name": stage_name,
            "status": status,
            "iteration_count": info["count"] if info else 0,
            "has_limit_restart": info["has_limit_restart"] if info else False,
        }
        # Attach Codex review summary to the review stage
        if stage_name == "review" and codex_summary:
            entry["codex_review"] = codex_summary
        result.append(entry)

    return result
