"""Claude subprocess invocation, output parsing, and text helpers.

Extracted from ``worker.py`` to keep the main pipeline module focused
on orchestration logic.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from botfarm.agent import AgentResult, ContextFillCallback
from botfarm.codex import CodexResult
from botfarm.process import terminate_process_group as _terminate_process_group

logger = logging.getLogger(__name__)

# Truncation limits (characters) for error messages and subprocess output
RESULT_TRUNCATE_CHARS = 200
DETAIL_TRUNCATE_CHARS = 500
CI_OUTPUT_TRUNCATE_CHARS = 2000

# Default context window size for per-turn fill calculation
DEFAULT_CONTEXT_WINDOW = 200_000


@dataclass
class ClaudeResult:
    """Parsed result from a ``claude -p --output-format json`` invocation."""

    session_id: str
    num_turns: int
    duration_seconds: float
    exit_subtype: str
    result_text: str
    is_error: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    total_cost_usd: float = 0.0
    context_fill_pct: float | None = None
    model_usage_json: str | None = None


def parse_claude_output(raw: str) -> ClaudeResult:
    """Parse the JSON output produced by ``claude -p --output-format json``.

    The expected shape is a JSON object with at least:
    ``session_id``, ``num_turns``, ``duration_ms``,
    ``is_error``, ``result``, and ``subtype``.

    Also extracts token usage data from the ``usage`` and ``modelUsage``
    objects when present, and computes ``context_fill_pct`` from the
    primary model's context window.
    """
    data = json.loads(raw)
    duration_ms = data.get("duration_ms", 0)

    # Token usage from top-level "usage" object
    usage = data.get("usage") or {}
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    cache_creation = usage.get("cache_creation_input_tokens", 0)

    total_cost_usd = data.get("total_cost_usd", 0.0) or 0.0

    # Per-model breakdown
    model_usage = data.get("modelUsage")
    model_usage_json = json.dumps(model_usage) if model_usage else None

    # Compute context fill % from the primary model's contextWindow
    context_fill_pct = _compute_context_fill(input_tokens, cache_creation, output_tokens, model_usage)

    return ClaudeResult(
        session_id=data.get("session_id", ""),
        num_turns=data.get("num_turns", 0),
        duration_seconds=duration_ms / 1000.0,
        exit_subtype=data.get("subtype", ""),
        result_text=data.get("result", ""),
        is_error=bool(data.get("is_error", False)),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_input_tokens=cache_read,
        cache_creation_input_tokens=cache_creation,
        total_cost_usd=total_cost_usd,
        context_fill_pct=context_fill_pct,
        model_usage_json=model_usage_json,
    )


def _compute_context_fill(
    input_tokens: int,
    cache_creation_input_tokens: int,
    output_tokens: int,
    model_usage: dict | None,
) -> float | None:
    """Compute context fill percentage from the primary model's context window.

    Returns ``None`` when ``modelUsage`` is absent or has no ``contextWindow``.
    """
    if not model_usage:
        return None
    # Pick the first model entry that has a contextWindow
    for _model, info in model_usage.items():
        context_window = info.get("contextWindow")
        if context_window and context_window > 0:
            unique_tokens = input_tokens + cache_creation_input_tokens + output_tokens
            return round(unique_tokens / context_window * 100, 2)
    return None


def _compute_turn_context_fill(
    usage: dict,
    context_window: int,
) -> float | None:
    """Compute context fill % from a single assistant turn's usage data.

    Unlike ``_compute_context_fill()`` (which uses cumulative data and
    excludes cache_read to avoid double-counting), this per-turn formula
    **includes** cache_read_input_tokens because per-API-call usage
    reports the full cached input read at each turn.
    """
    input_tokens = usage.get("input_tokens", 0)
    cache_creation = usage.get("cache_creation_input_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)

    total = input_tokens + cache_creation + cache_read + output_tokens
    if context_window <= 0 or total == 0:
        return None
    return round(total / context_window * 100, 2)


def parse_stream_json_result(result_data: dict) -> ClaudeResult:
    """Parse a stream-json ``result`` message into a ``ClaudeResult``.

    The ``result`` message in stream-json contains the same fields as the
    single JSON blob from ``--output-format json``.  Finds and parses the
    ``result`` line from NDJSON stream output.
    """
    duration_ms = result_data.get("duration_ms", 0)

    usage = result_data.get("usage") or {}
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    cache_creation = usage.get("cache_creation_input_tokens", 0)

    total_cost_usd = result_data.get("total_cost_usd", 0.0) or 0.0

    model_usage = result_data.get("modelUsage")
    model_usage_json = json.dumps(model_usage) if model_usage else None

    context_fill_pct = _compute_context_fill(
        input_tokens, cache_creation, output_tokens, model_usage,
    )

    return ClaudeResult(
        session_id=result_data.get("session_id", ""),
        num_turns=result_data.get("num_turns", 0),
        duration_seconds=duration_ms / 1000.0,
        exit_subtype=result_data.get("subtype", ""),
        result_text=result_data.get("result", ""),
        is_error=bool(result_data.get("is_error", False)),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_input_tokens=cache_read,
        cache_creation_input_tokens=cache_creation,
        total_cost_usd=total_cost_usd,
        context_fill_pct=context_fill_pct,
        model_usage_json=model_usage_json,
    )


def _parse_ndjson_stream(
    proc_stdout,
    *,
    on_context_fill: ContextFillCallback | None = None,
    log_fh=None,
    context_window: int = DEFAULT_CONTEXT_WINDOW,
) -> tuple[list[str], ClaudeResult | None]:
    """Read NDJSON lines from Claude's stdout stream.

    Parses each line as JSON, tracks assistant turns for context fill
    callbacks, and returns when a ``result`` message is received.

    Returns ``(stdout_lines, claude_result)``.  *claude_result* is
    ``None`` if no result message was received before the stream ended.
    """
    stdout_lines: list[str] = []
    turn_number = 0
    claude_result: ClaudeResult | None = None

    for line in proc_stdout:
        stdout_lines.append(line)

        if log_fh is not None:
            try:
                log_fh.write(line)
                log_fh.flush()
            except OSError:
                pass

        stripped = line.strip()
        if not stripped:
            continue

        try:
            event = json.loads(stripped)
        except json.JSONDecodeError:
            logger.debug("Skipping non-JSON stream line: %s", stripped[:100])
            continue

        msg_type = event.get("type")

        if msg_type == "assistant":
            turn_number += 1
            usage = (event.get("message") or {}).get("usage")
            if usage and on_context_fill is not None:
                fill_pct = _compute_turn_context_fill(usage, context_window)
                if fill_pct is not None:
                    try:
                        on_context_fill(turn_number, fill_pct)
                    except Exception:
                        logger.debug(
                            "on_context_fill callback failed", exc_info=True,
                        )

        elif msg_type == "result":
            claude_result = parse_stream_json_result(event)
            break

    return stdout_lines, claude_result


def _finalize_streaming_result(
    proc: subprocess.Popen,
    cmd: list[str],
    *,
    stdout_lines: list[str],
    stderr_lines: list[str],
    claude_result: ClaudeResult | None,
    timed_out: threading.Event,
    log_fh,
    timeout: float | None,
) -> ClaudeResult:
    """Finalize a streaming Claude process: write logs, check errors, return result.

    Appends stderr to the log file (if open), checks for timeout and
    non-zero exit codes, and returns the parsed ``ClaudeResult``.

    Raises ``TimeoutError``, ``subprocess.CalledProcessError``, or
    ``RuntimeError`` when the process did not produce a valid result.
    """
    stdout_text = "".join(stdout_lines)
    stderr_text = "".join(stderr_lines)

    if log_fh is not None:
        try:
            if stderr_text:
                log_fh.write("\n--- STDERR ---\n")
                log_fh.write(stderr_text)
        except OSError:
            pass
        finally:
            log_fh.close()

    if timed_out.is_set() and proc.returncode in (-15, -9, 137, 143, None):
        raise TimeoutError(
            f"claude streaming process timed out after {timeout}s"
        )

    if proc.returncode != 0 and claude_result is None:
        logger.error(
            "claude (streaming) exited with code %d\nstderr: %s",
            proc.returncode,
            stderr_text[:DETAIL_TRUNCATE_CHARS] if stderr_text else "(empty)",
        )
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=stdout_text, stderr=stderr_text,
        )

    if proc.returncode != 0 and claude_result is not None:
        logger.warning(
            "claude (streaming) exited with code %d after result was parsed "
            "(likely killed during MCP server cleanup)",
            proc.returncode,
        )

    if claude_result is None:
        raise RuntimeError(
            "claude stream-json produced no result message; "
            f"stdout length={len(stdout_text)}"
        )

    return claude_result


def run_claude_streaming(
    prompt: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    timeout: float | None = None,
    mcp_config: str | None = None,
    auth_mode: str = "oauth",
    model: str | None = None,
    effort: str | None = None,
    context_window: int | None = None,
) -> ClaudeResult:
    """Run ``claude`` with streaming output and per-turn context fill callbacks.

    Uses ``--output-format stream-json --verbose`` to receive NDJSON
    output line-by-line.  On each ``assistant`` message, computes the
    current context fill % and invokes *on_context_fill(turn_number,
    context_fill_pct)* if provided.

    The final ``result`` message is parsed into a ``ClaudeResult``
    identical to what ``parse_claude_output()`` returns.

    Stderr is drained on a separate thread to prevent pipe deadlocks.

    If *timeout* is given (in seconds), a watchdog thread kills the
    process when the deadline is exceeded and ``TimeoutError`` is raised.
    If *log_file* is given, each NDJSON line is flushed to disk in
    real-time so that external tools can tail the log during execution.

    Raises ``FileNotFoundError`` if the ``claude`` binary is not on PATH.
    """
    claude_bin = shutil.which("claude")
    if not claude_bin:
        raise FileNotFoundError(
            "Cannot find 'claude' on PATH. "
            "Ensure Claude Code is installed and ~/.local/bin is in PATH. "
            "For nohup usage: PATH=$HOME/.local/bin:$PATH nohup botfarm run &"
        )
    cmd = [
        claude_bin,
        "-p",
        "--output-format", "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
        "--max-turns", str(max_turns),
    ]
    if model:
        cmd.extend(["--model", model])
    if effort:
        cmd.extend(["--effort", effort])
    # In --bare mode, Claude Code skips hooks, auto-memory, and CLAUDE.md
    # auto-discovery; --add-dir compensates for the last of these.
    if auth_mode == "api_key":
        cmd.append("--bare")
        cmd.extend(["--add-dir", str(cwd)])
    # Write MCP config to a temp file so the API key isn't visible in ps output
    mcp_config_path: str | None = None
    if mcp_config:
        fd, mcp_config_path = tempfile.mkstemp(suffix=".json", prefix="mcp-")
        with os.fdopen(fd, "w") as f:
            f.write(mcp_config)
        cmd.extend(["--mcp-config", mcp_config_path])
    logger.info("Running claude (%s, streaming) with max_turns=%d in %s", claude_bin, max_turns, cwd)

    subprocess_env = None
    if env:
        subprocess_env = {**os.environ, **env}

    try:
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
        proc.stdin.write(prompt)
        proc.stdin.close()

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
                _terminate_process_group(proc)

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

        # Parse NDJSON stream with context fill tracking
        try:
            stdout_lines, claude_result = _parse_ndjson_stream(
                proc.stdout, on_context_fill=on_context_fill, log_fh=log_fh,
                context_window=context_window if context_window is not None else DEFAULT_CONTEXT_WINDOW,
            )
        finally:
            cancel_watchdog.set()
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=2)

        # Terminate the process group (Claude + MCP server children) if still alive
        _terminate_process_group(proc)
        proc.wait()
        stderr_thread.join(timeout=5)

        return _finalize_streaming_result(
            proc, cmd,
            stdout_lines=stdout_lines,
            stderr_lines=stderr_lines,
            claude_result=claude_result,
            timed_out=timed_out,
            log_fh=log_fh,
            timeout=timeout,
        )
    finally:
        if mcp_config_path:
            try:
                os.unlink(mcp_config_path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Stage result
# ---------------------------------------------------------------------------


@dataclass
class StageResult:
    """Outcome of a single pipeline stage."""

    stage: str
    success: bool
    agent_result: AgentResult | None = None
    pr_url: str | None = None
    error: str | None = None
    review_approved: bool | None = None
    claude_review_approved: bool | None = None
    # Secondary agent result for dual-reviewer flows (e.g. Codex review
    # running alongside Claude review).
    secondary_agent_result: AgentResult | None = None
    # QA report fields (populated when result_parser == "qa_report")
    qa_report_text: str | None = None
    qa_bugs: list[dict] | None = None
    qa_passed: bool | None = None

    # ------------------------------------------------------------------
    # Backward-compatibility read-only properties
    # ------------------------------------------------------------------

    @property
    def claude_result(self) -> ClaudeResult | None:
        """Reconstruct a :class:`ClaudeResult` from *agent_result*.

        .. deprecated:: Use ``agent_result`` directly.
        """
        ar = self.agent_result
        if ar is None:
            return None
        return ClaudeResult(
            session_id=ar.session_id,
            num_turns=ar.num_turns,
            duration_seconds=ar.duration_seconds,
            exit_subtype=ar.extra.get("exit_subtype", ""),
            result_text=ar.result_text,
            is_error=ar.is_error,
            input_tokens=ar.input_tokens,
            output_tokens=ar.output_tokens,
            cache_read_input_tokens=ar.extra.get("cache_read_input_tokens", 0),
            cache_creation_input_tokens=ar.extra.get("cache_creation_input_tokens", 0),
            total_cost_usd=ar.cost_usd,
            context_fill_pct=ar.context_fill_pct,
            model_usage_json=ar.extra.get("model_usage_json"),
        )

    @property
    def codex_result(self) -> CodexResult | None:
        """Reconstruct a :class:`CodexResult` from *secondary_agent_result*.

        .. deprecated:: Use ``secondary_agent_result`` directly.
        """
        ar = self.secondary_agent_result
        if ar is None:
            return None
        return CodexResult(
            thread_id=ar.session_id,
            num_turns=ar.num_turns,
            duration_seconds=ar.duration_seconds,
            result_text=ar.result_text,
            is_error=ar.is_error,
            input_tokens=ar.input_tokens,
            output_tokens=ar.output_tokens,
            cached_input_tokens=ar.extra.get("cache_read_input_tokens", 0),
            model=ar.extra.get("model", ""),
        )


# ---------------------------------------------------------------------------
# Claude invocation helper
# ---------------------------------------------------------------------------


def _invoke_claude(
    prompt: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    timeout: float | None = None,
    mcp_config: str | None = None,
    auth_mode: str = "oauth",
) -> ClaudeResult:
    """Run Claude via the streaming runner.

    Always uses ``run_claude_streaming()`` which writes logs incrementally
    and supports watchdog-based timeouts.  The *on_context_fill* callback
    is optional — when ``None``, streaming still works but per-turn
    context fill updates are simply skipped.
    """
    return run_claude_streaming(
        prompt, cwd=cwd, max_turns=max_turns,
        log_file=log_file, env=env, on_context_fill=on_context_fill,
        timeout=timeout, mcp_config=mcp_config, auth_mode=auth_mode,
    )

_NO_PR_NEEDED_RE = re.compile(r"NO_PR_NEEDED:\s*(.+)", re.IGNORECASE | re.DOTALL)


def _detect_no_pr_needed(text: str) -> str | None:
    """Check if the agent signalled that no PR is needed.

    Looks for ``NO_PR_NEEDED: <explanation>`` in *text* (case-insensitive).
    Returns the explanation string (stripped, capped at 500 chars) or ``None``.
    """
    if not text:
        return None
    match = _NO_PR_NEEDED_RE.search(text)
    if not match:
        return None
    return match.group(1).strip()[:DETAIL_TRUNCATE_CHARS] or None


_PR_URL_RE = re.compile(
    r"https://github\.com/([\w.-]+)/([\w.-]+)/pull/(\d+)",
)


def _extract_pr_url(text: str) -> str | None:
    """Try to extract a GitHub PR URL from text.

    Looks for patterns like https://github.com/<owner>/<repo>/pull/<number>.
    """
    match = _PR_URL_RE.search(text)
    return match.group(0) if match else None


def _extract_pr_url_from_log(log_file: Path | None) -> str | None:
    """Scan a stage log file for a GitHub PR URL.

    The log contains the full NDJSON session transcript, including tool
    results from ``gh pr create``.  Returns the **last** PR URL found
    (earlier matches may be unrelated URLs from ticket context), or
    ``None`` if the file is missing / unreadable / contains no URL.
    """
    if log_file is None:
        return None
    try:
        text = log_file.read_text(errors="replace")
    except OSError:
        logger.debug("Cannot read log file %s for PR URL scan", log_file, exc_info=True)
        return None
    matches = _PR_URL_RE.findall(text)
    if not matches:
        return None
    # findall returns tuples of groups; reconstruct the full URL from the last match
    owner, repo, number = matches[-1]
    return f"https://github.com/{owner}/{repo}/pull/{number}"


def _find_open_pr_url(
    branch: str, cwd: str | Path,
    *, env: dict[str, str] | None = None,
) -> str | None:
    """Find the open PR URL for *branch* via ``gh``.

    Uses ``gh pr list --head <branch> --state open`` as the primary lookup,
    falling back to ``gh pr view`` filtered by open state.
    """
    subprocess_env = {**os.environ, **env} if env else None
    # Primary: targeted query for open PRs on this branch
    try:
        proc = subprocess.run(
            ["gh", "pr", "list", "--head", branch, "--state", "open",
             "--json", "url", "--limit", "1"],
            capture_output=True, text=True, cwd=str(cwd), timeout=15,
            env=subprocess_env,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            prs = json.loads(proc.stdout)
            if prs:
                url = prs[0].get("url", "")
                if "github.com" in url and "/pull/" in url:
                    return url
    except Exception:
        logger.debug("gh pr list failed for branch %s", branch, exc_info=True)

    # Fallback: gh pr view filtered by open state
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", branch, "--json", "url,state"],
            capture_output=True, text=True, cwd=str(cwd), timeout=15,
            env=subprocess_env,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            data = json.loads(proc.stdout)
            if data.get("state") == "OPEN":
                url = data.get("url", "")
                if "github.com" in url and "/pull/" in url:
                    return url
    except Exception:
        logger.debug("gh pr view fallback failed for branch %s", branch, exc_info=True)
    return None


def _gh_pr_view_url(cwd: str | Path, *, env: dict[str, str] | None = None) -> str | None:
    """Get the open PR URL for the current branch.

    Resolves the current branch via ``git rev-parse`` then delegates to
    :func:`_find_open_pr_url`.
    """
    subprocess_env = {**os.environ, **env} if env else None
    # Get the current branch name
    try:
        branch_proc = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, cwd=str(cwd), timeout=10,
            env=subprocess_env,
        )
        if branch_proc.returncode != 0 or not branch_proc.stdout.strip():
            logger.debug("Could not determine current branch for PR lookup")
            return None
        branch = branch_proc.stdout.strip()
    except Exception:
        logger.debug("Failed to get current branch", exc_info=True)
        return None

    return _find_open_pr_url(branch, cwd, env=env)


def _parse_pr_url(pr_url: str) -> tuple[str, str, str]:
    """Extract (owner, repo, number) from a GitHub PR URL.

    Raises ``ValueError`` if the URL doesn't match the expected pattern.
    """
    match = _PR_URL_RE.match(pr_url)
    if not match:
        raise ValueError(f"Cannot parse PR URL: {pr_url}")
    return match.group(1), match.group(2), match.group(3)


def _parse_review_approved(text: str) -> bool:
    """Determine whether a review result indicates approval or changes requested.

    Searches the **entire** ``text`` for structured verdict markers
    (``VERDICT: APPROVED`` or ``VERDICT: CHANGES_REQUESTED``) using a
    case-insensitive regex.  Falls back to ``gh pr review`` command
    detection, then keyword heuristics on the last 500 characters.

    Returns ``True`` if the review text signals approval, ``False`` if
    changes are requested, and ``False`` as a conservative default when
    the signal is ambiguous.
    """
    # 1. Structured verdict marker — strongest signal, full text, case-insensitive.
    #    Use findall + last match so early occurrences (e.g. quoted from test code
    #    the reviewer is discussing) don't shadow the real verdict at the end.
    verdict_matches = re.findall(
        r"VERDICT:\s*(APPROVED|CHANGES_REQUESTED)", text, re.IGNORECASE
    )
    if verdict_matches:
        return verdict_matches[-1].upper() == "APPROVED"

    # 2. gh pr review command — search full text, check reject first (conservative)
    if re.search(r"gh\s+pr\s+review\s+.*--request-changes", text):
        return False
    if re.search(r"gh\s+pr\s+review\s+.*--approve", text):
        return True

    logger.warning(
        "No VERDICT marker or gh pr review command found; "
        "falling back to keyword heuristics"
    )

    # 3. Keyword heuristics on the tail to avoid false positives from
    #    quoted content earlier in the output.
    tail = text[-DETAIL_TRUNCATE_CHARS:]
    lower = tail.lower()
    # gh --approve/--request-changes without the full command prefix
    if "--approve" in lower:
        return True
    approve_patterns = [
        "approve the pr",
        "i approve",
        "lgtm",
        "looks good to me",
        "approved the pr",
        "approved this pr",
        "pr is approved",
    ]
    changes_patterns = [
        "request changes",
        "changes requested",
        "--request-changes",
        "requesting changes",
        "needs changes",
    ]
    for pattern in changes_patterns:
        if pattern in lower:
            return False
    for pattern in approve_patterns:
        if pattern in lower:
            return True
    # Conservative default: assume changes are needed
    return False


def _parse_qa_report(text: str) -> tuple[str | None, list[dict], bool | None]:
    """Parse a QA agent's output for report text, bug entries, and verdict.

    Extracts:
    - Report text between ``QA_REPORT_START`` / ``QA_REPORT_END`` markers
    - Bug entries between ``BUG_START`` / ``BUG_END`` markers (title, severity, description)
    - Overall verdict from the ``Verdict:`` line in the report

    Returns a tuple of ``(report_text, bugs, passed)`` where:
    - ``report_text`` is the text between the report markers, or ``None`` if not found
    - ``bugs`` is a list of parsed bug dicts (may be empty)
    - ``passed`` is ``True`` if verdict is PASSED, ``False`` if FAILED, ``None`` if ambiguous

    Handles partial/missing markers and malformed bug entries gracefully.
    """
    # Extract report text between QA_REPORT_START / QA_REPORT_END
    report_text: str | None = None
    report_match = re.search(
        r"QA_REPORT_START\s*(.*?)\s*QA_REPORT_END",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if report_match:
        report_text = report_match.group(1).strip()

    # Parse verdict from the report text (or full text as fallback)
    search_text = report_text if report_text is not None else text
    passed: bool | None = None
    verdict_match = re.search(
        r"^Verdict:\s*(PASSED|FAILED)\s*$", search_text, re.MULTILINE | re.IGNORECASE
    )
    if verdict_match:
        passed = verdict_match.group(1).upper() == "PASSED"

    # Extract bug entries between BUG_START / BUG_END markers
    bugs: list[dict] = []
    for bug_block in re.finditer(
        r"BUG_START\s*(.*?)\s*BUG_END", text, re.DOTALL | re.IGNORECASE
    ):
        block = bug_block.group(1).strip()
        bug: dict = {}

        title_match = re.search(r"^Title:\s*(.+)$", block, re.MULTILINE)
        if title_match:
            bug["title"] = title_match.group(1).strip()

        severity_match = re.search(r"^Severity:\s*(.+)$", block, re.MULTILINE)
        if severity_match:
            bug["severity"] = severity_match.group(1).strip().lower()

        desc_match = re.search(r"^Description:\s*\n(.*)", block, re.DOTALL | re.MULTILINE)
        if not desc_match:
            # Fallback: inline description on the same line
            desc_match = re.search(r"^Description:\s+(.+)", block, re.MULTILINE)
        if desc_match:
            bug["description"] = desc_match.group(1).strip()

        if bug:
            bugs.append(bug)

    return report_text, bugs, passed


def _make_stage_log_path(
    log_dir: Path | None,
    stage: str,
    iteration: int = 1,
) -> Path | None:
    """Build a timestamped log file path for a pipeline stage.

    Returns ``None`` when *log_dir* is not configured, which disables
    per-stage log file writing.
    """
    if log_dir is None:
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    suffix = f"-iter{iteration}" if iteration > 1 else ""
    return log_dir / f"{stage}{suffix}-{ts}.log"


def _write_subprocess_log(
    log_file: Path,
    stdout: str | None,
    stderr: str | None,
) -> None:
    """Write captured subprocess stdout/stderr to *log_file*."""
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("w") as fh:
            if stdout:
                fh.write(stdout)
            if stderr:
                fh.write("\n--- STDERR ---\n")
                fh.write(stderr)
        logger.info("Wrote subprocess log to %s", log_file)
    except OSError:
        logger.warning("Failed to write subprocess log to %s", log_file, exc_info=True)


def _check_pr_merged(pr_url: str, cwd: str | Path, *, env: dict[str, str] | None = None) -> bool:
    """Check whether a PR has been merged by querying its state.

    Returns ``True`` if the PR state is ``MERGED``, ``False`` otherwise.
    """
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json", "state", "--jq", ".state"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=15,
            env=env,
        )
        if proc.returncode == 0:
            return proc.stdout.strip().upper() == "MERGED"
    except Exception:
        logger.debug("Failed to check PR state for %s", pr_url, exc_info=True)
    return False


def _recover_pr_url(
    cwd: str | Path,
    *, env: dict[str, str] | None = None,
) -> str | None:
    """Recover the PR URL for a resumed pipeline.

    Uses ``gh pr list --head <branch> --state open`` to find the open PR
    for the current branch, falling back to ``gh pr view`` filtered by
    open state.
    """
    subprocess_env = {**os.environ, **env} if env else None
    # Get the current branch name
    try:
        branch_proc = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, cwd=str(cwd), timeout=10,
            env=subprocess_env,
        )
        if branch_proc.returncode != 0 or not branch_proc.stdout.strip():
            return None
        branch = branch_proc.stdout.strip()
    except Exception:
        return None

    url = _find_open_pr_url(branch, cwd, env=env)
    if url:
        logger.info("Recovered PR URL from gh: %s", url)
    return url


# ---------------------------------------------------------------------------
# Preflight check
# ---------------------------------------------------------------------------


def check_claude_available() -> tuple[bool, str]:
    """Check if claude binary exists and is runnable. Returns (ok, message)."""
    try:
        result = subprocess.run(
            ["claude", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            version = result.stdout.strip()
            path = shutil.which("claude") or "claude"
            return True, f"{path} ({version})"
        return False, f"claude --version exited with code {result.returncode}"
    except FileNotFoundError:
        return False, "claude binary not found on PATH"
    except subprocess.TimeoutExpired:
        return False, "claude --version timed out"
    except OSError as exc:
        return False, f"claude check failed: {exc}"
