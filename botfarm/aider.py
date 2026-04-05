"""Aider CLI invocation and output parsing.

Encapsulates Aider subprocess management and output parsing for use as
a reference third-party adapter in the botfarm agent abstraction layer.

Aider is invoked in non-interactive message mode via ``--message``.
Output is captured line-by-line; token counts and session cost are
extracted from Aider's summary lines.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
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
class AiderResult:
    """Parsed result from an Aider CLI invocation."""

    duration_seconds: float
    result_text: str
    is_error: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    model: str = ""


# ---------------------------------------------------------------------------
# Output parsing
# ---------------------------------------------------------------------------

# Aider token/cost line patterns:
#   "Tokens: 4.2k sent, 892 received. Cost: $0.01 message, $0.03 session."
#   "Tokens: 4,225 sent, 892 received."
_TOKEN_PATTERN = re.compile(
    r"Tokens:\s+([\d,.]+k?)\s+sent,\s+([\d,.]+k?)\s+received",
    re.IGNORECASE,
)
_COST_PATTERN = re.compile(
    r"Cost:\s+\$([\d.]+)\s+message,\s+\$([\d.]+)\s+session",
    re.IGNORECASE,
)


def _parse_token_count(s: str) -> int:
    """Parse a token count string like ``'4.2k'`` or ``'4,225'`` to int."""
    s = s.strip().replace(",", "")
    if s.lower().endswith("k"):
        return int(float(s[:-1]) * 1000)
    return int(float(s))


def parse_aider_output(lines: list[str]) -> AiderResult:
    """Parse Aider's stdout into an :class:`AiderResult`.

    Token counts are accumulated across all summary lines (Aider prints
    per-message counts, not running totals).  Session cost uses the *last*
    value because Aider's ``$session`` figure is already cumulative.
    The full output is preserved verbatim in ``result_text``.
    """
    input_tokens = 0
    output_tokens = 0
    session_cost = 0.0

    for line in lines:
        token_match = _TOKEN_PATTERN.search(line)
        if token_match:
            input_tokens += _parse_token_count(token_match.group(1))
            output_tokens += _parse_token_count(token_match.group(2))

        cost_match = _COST_PATTERN.search(line)
        if cost_match:
            session_cost = float(cost_match.group(2))

    return AiderResult(
        duration_seconds=0.0,  # filled in by caller
        result_text="".join(lines).strip(),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=session_cost,
    )


# ---------------------------------------------------------------------------
# Aider runner
# ---------------------------------------------------------------------------


def run_aider(
    prompt: str,
    *,
    cwd: str | Path,
    model: str | None = None,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> AiderResult:
    """Invoke Aider as a subprocess in non-interactive message mode.

    Command::

        aider --yes-always --no-suggest-shell-commands --no-pretty
              [--model <model>] --message <prompt>

    Uses the same watchdog-thread timeout pattern as other adapter runners.
    On non-zero exit or timeout the result has ``is_error=True`` rather
    than raising an exception.

    Raises :class:`FileNotFoundError` if the ``aider`` binary is not on PATH.
    """
    aider_bin = shutil.which("aider")
    if not aider_bin:
        raise FileNotFoundError(
            "Cannot find 'aider' on PATH. "
            "Ensure Aider is installed: pip install aider-chat"
        )

    cmd = [
        aider_bin,
        "--yes-always",
        "--no-suggest-shell-commands",
        "--no-pretty",
        "--no-auto-lint",
        "--no-auto-test",
    ]
    if model:
        cmd.extend(["--model", model])
    cmd.extend(["--message", prompt])

    logger.info("Running aider in %s", cwd)

    subprocess_env = None
    if env:
        subprocess_env = {**os.environ, **env}

    start_time = time.monotonic()

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(cwd),
        env=subprocess_env,
        start_new_session=True,
    )

    # Drain stderr on a background thread to prevent deadlock.
    stderr_lines: list[str] = []

    def _drain_stderr() -> None:
        for line in proc.stderr:
            stderr_lines.append(line)

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    # Watchdog: kill the process if it exceeds the timeout.
    cancel_watchdog = threading.Event()
    timed_out = threading.Event()

    def _watchdog() -> None:
        if not cancel_watchdog.wait(timeout):
            timed_out.set()
            terminate_process_group(proc)

    watchdog_thread: threading.Thread | None = None
    if timeout is not None and timeout > 0:
        watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
        watchdog_thread.start()

    # Open log file for real-time writing (if configured).
    log_fh = None
    if log_file is not None:
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_fh = log_file.open("w")
        except OSError:
            logger.warning("Failed to open log file %s for streaming", log_file, exc_info=True)

    # Read stdout line-by-line.
    stdout_lines: list[str] = []

    try:
        for line in proc.stdout:
            stdout_lines.append(line)
            if log_fh is not None:
                try:
                    log_fh.write(line)
                    log_fh.flush()
                except OSError:
                    pass
    finally:
        cancel_watchdog.set()
        if watchdog_thread is not None:
            watchdog_thread.join(timeout=2)

    # Terminate the process group if still alive.
    terminate_process_group(proc)
    proc.wait()
    stderr_thread.join(timeout=5)

    duration = time.monotonic() - start_time
    stderr_text = "".join(stderr_lines)

    # Append stderr to log file and close.
    if log_fh is not None:
        try:
            if stderr_text:
                log_fh.write("\n--- STDERR ---\n")
                log_fh.write(stderr_text)
        except OSError:
            pass
        finally:
            log_fh.close()

    # Parse output.
    result = parse_aider_output(stdout_lines)
    result.duration_seconds = duration
    result.model = model or ""

    if timed_out.is_set():
        result.is_error = True
        logger.warning("Aider process timed out after %.1fs", timeout)

    if proc.returncode != 0:
        result.is_error = True
        logger.warning(
            "Aider exited with code %d\nstderr: %s",
            proc.returncode,
            stderr_text[:500] if stderr_text else "(empty)",
        )

    return result


# ---------------------------------------------------------------------------
# Preflight check
# ---------------------------------------------------------------------------


def check_aider_available() -> tuple[bool, str]:
    """Check if aider binary exists and is runnable. Returns (ok, message)."""
    try:
        result = subprocess.run(
            ["aider", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            version = result.stdout.strip()
            return True, f"aider {version}"
        return False, f"aider --version exited with code {result.returncode}"
    except FileNotFoundError:
        return False, "aider binary not found on PATH"
    except subprocess.TimeoutExpired:
        return False, "aider --version timed out"
    except OSError as exc:
        return False, f"aider check failed: {exc}"
