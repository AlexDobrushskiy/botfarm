"""Codex CLI invocation and JSONL output parsing.

Parallel to how ``worker.py`` invokes Claude via ``run_claude_streaming()``,
this module encapsulates Codex subprocess management and real-time JSONL
event parsing for use in the review orchestration pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from botfarm.process import terminate_process_group

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class CodexResult:
    """Parsed result from a Codex CLI JSONL invocation."""

    thread_id: str
    num_turns: int
    duration_seconds: float
    result_text: str  # last agent_message text
    is_error: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0


# ---------------------------------------------------------------------------
# JSONL event parsing
# ---------------------------------------------------------------------------


def parse_codex_jsonl(lines: list[str]) -> CodexResult:
    """Parse a sequence of Codex JSONL event lines into a ``CodexResult``.

    Handles the following event types:
    - ``thread.started``  -> extract ``thread_id``
    - ``turn.started``    -> increment turn counter
    - ``item.completed``  -> collect ``agent_message`` text
    - ``turn.completed``  -> accumulate token usage
    - ``error`` / ``turn.failed`` -> mark ``is_error``

    Unparseable lines are silently skipped.
    """
    thread_id = ""
    num_turns = 0
    last_agent_message = ""
    is_error = False
    input_tokens = 0
    output_tokens = 0
    cached_input_tokens = 0

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            continue
        try:
            event = json.loads(stripped)
        except json.JSONDecodeError:
            logger.warning("Skipping unparseable Codex JSONL line: %s", stripped[:120])
            continue

        event_type = event.get("type")

        if event_type == "thread.started":
            thread_id = event.get("thread_id", "")

        elif event_type == "turn.started":
            num_turns += 1

        elif event_type == "item.completed":
            item = event.get("item", {})
            if item.get("type") == "agent_message":
                last_agent_message = item.get("text", "")
            elif item.get("type") == "reasoning":
                logger.debug("Codex reasoning: %s", item.get("text", "")[:200])

        elif event_type == "turn.completed":
            usage = event.get("usage", {})
            input_tokens += usage.get("input_tokens", 0)
            output_tokens += usage.get("output_tokens", 0)
            cached_input_tokens += usage.get("cached_input_tokens", 0)

        elif event_type in ("error", "turn.failed"):
            is_error = True

    return CodexResult(
        thread_id=thread_id,
        num_turns=num_turns,
        duration_seconds=0.0,  # filled in by caller
        result_text=last_agent_message,
        is_error=is_error,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
    )


# ---------------------------------------------------------------------------
# Streaming Codex runner
# ---------------------------------------------------------------------------


def run_codex_streaming(
    prompt: str,
    *,
    cwd: str | Path,
    model: str | None = None,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> CodexResult:
    """Invoke Codex as a subprocess with JSONL streaming.

    Command:
        codex -a never -s workspace-write [-m <model>] -C <cwd> exec --ephemeral --json -

    The prompt is fed via stdin (the ``-`` argument to ``exec``).

    Uses the same watchdog-thread timeout pattern as
    ``worker.run_claude_streaming()``.

    On non-zero exit or timeout the result has ``is_error=True`` rather
    than raising an exception.
    """
    cmd = [
        "codex",
        "-a", "never",
        "-s", "workspace-write",
    ]
    if model:
        cmd.extend(["-m", model])
    cmd.extend(["-C", str(cwd), "exec", "--ephemeral", "--json", "-"])

    logger.info("Running codex (streaming) in %s", cwd)

    subprocess_env = None
    if env:
        subprocess_env = {**os.environ, **env}

    start_time = time.monotonic()

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(cwd),
        env=subprocess_env,
        start_new_session=True,
    )

    # Write prompt and close stdin
    try:
        proc.stdin.write(prompt)
        proc.stdin.close()
    except BrokenPipeError:
        logger.warning("Codex process closed stdin before prompt was written")

    # Drain stderr on a background thread to prevent deadlock
    stderr_lines: list[str] = []

    def _drain_stderr():
        for line in proc.stderr:
            stderr_lines.append(line)

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    # Watchdog: kill the process if it exceeds the timeout.
    cancel_watchdog = threading.Event()
    timed_out = threading.Event()

    def _watchdog():
        if not cancel_watchdog.wait(timeout):
            timed_out.set()
            terminate_process_group(proc)

    watchdog_thread: threading.Thread | None = None
    if timeout is not None and timeout > 0:
        watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
        watchdog_thread.start()

    # Open log file for real-time writing (if configured)
    log_fh = None
    if log_file is not None:
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_fh = log_file.open("w")
        except OSError:
            logger.warning("Failed to open log file %s for streaming", log_file, exc_info=True)

    # Read stdout line-by-line (JSONL)
    stdout_lines: list[str] = []

    try:
        for line in proc.stdout:
            stdout_lines.append(line)

            # Write to log file in real-time
            if log_fh is not None:
                try:
                    log_fh.write(line)
                    log_fh.flush()
                except OSError:
                    pass
    finally:
        # Cancel watchdog
        cancel_watchdog.set()
        if watchdog_thread is not None:
            watchdog_thread.join(timeout=2)

    # Terminate the process group if still alive
    terminate_process_group(proc)
    proc.wait()
    stderr_thread.join(timeout=5)

    duration = time.monotonic() - start_time
    stderr_text = "".join(stderr_lines)

    # Append stderr to log file and close
    if log_fh is not None:
        try:
            if stderr_text:
                log_fh.write("\n--- STDERR ---\n")
                log_fh.write(stderr_text)
        except OSError:
            pass
        finally:
            log_fh.close()

    # Parse collected JSONL lines
    result = parse_codex_jsonl(stdout_lines)
    result.duration_seconds = duration

    # Mark errors for timeout or non-zero exit
    if timed_out.is_set():
        result.is_error = True
        logger.warning("Codex process timed out after %.1fs", timeout)

    if proc.returncode != 0:
        result.is_error = True
        logger.warning(
            "Codex exited with code %d\nstderr: %s",
            proc.returncode,
            stderr_text[:500] if stderr_text else "(empty)",
        )

    return result


# ---------------------------------------------------------------------------
# Preflight check
# ---------------------------------------------------------------------------


def check_codex_available() -> tuple[bool, str]:
    """Check if codex binary exists and is runnable. Returns (ok, message)."""
    try:
        result = subprocess.run(
            ["codex", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            version = result.stdout.strip()
            return True, f"codex {version}"
        return False, f"codex --version exited with code {result.returncode}"
    except FileNotFoundError:
        return False, "codex binary not found on PATH"
    except subprocess.TimeoutExpired:
        return False, "codex --version timed out"
    except OSError as exc:
        return False, f"codex check failed: {exc}"
