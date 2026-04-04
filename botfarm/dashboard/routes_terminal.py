"""WebSocket terminal endpoint — bridges xterm.js to a server-side pty."""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import pty
import select
import signal
import struct
import termios
import time

from fastapi import APIRouter, Request, WebSocket
from starlette.websockets import WebSocketState

logger = logging.getLogger(__name__)

router = APIRouter()

MAX_CONCURRENT_SESSIONS = 2
IDLE_TIMEOUT_SECONDS = 15 * 60  # 15 minutes

_active_sessions: int = 0
_sessions_lock = asyncio.Lock()


async def _increment_sessions() -> bool:
    """Try to claim a session slot. Returns False if at capacity."""
    global _active_sessions
    async with _sessions_lock:
        if _active_sessions >= MAX_CONCURRENT_SESSIONS:
            return False
        _active_sessions += 1
        return True


async def _decrement_sessions() -> None:
    global _active_sessions
    async with _sessions_lock:
        _active_sessions = max(0, _active_sessions - 1)


def _is_terminal_enabled(app_state) -> bool:
    """Check if the terminal feature is enabled in config."""
    cfg = getattr(app_state, "botfarm_config", None)
    if cfg is None:
        return False  # Default to disabled when no config
    return getattr(cfg.dashboard, "terminal_enabled", False)


def _cleanup_child(child_pid: int, master_fd: int) -> None:
    """Kill child process and close master fd."""
    try:
        os.close(master_fd)
    except OSError:
        pass
    try:
        os.kill(child_pid, signal.SIGTERM)
    except OSError:
        pass
    try:
        pid, _ = os.waitpid(child_pid, os.WNOHANG)
        if pid == 0:
            # Child still running — give it a moment then force-kill
            time.sleep(0.1)
            os.kill(child_pid, signal.SIGKILL)
            os.waitpid(child_pid, 0)
    except (OSError, ChildProcessError):
        pass


def _pty_read(fd: int) -> str | None:
    """Blocking read from pty master fd. Returns None on EOF."""
    ready, _, _ = select.select([fd], [], [], 0.1)
    if not ready:
        return ""
    try:
        data = os.read(fd, 4096)
        if not data:
            return None
        return data.decode("utf-8", errors="replace")
    except OSError:
        return None


def _pty_write(fd: int, data: bytes) -> None:
    """Write data to pty master fd."""
    while data:
        n = os.write(fd, data)
        data = data[n:]


@router.websocket("/ws/terminal")
async def ws_terminal(ws: WebSocket):
    """WebSocket endpoint that spawns a pty-backed shell session."""
    if not _is_terminal_enabled(ws.app.state):
        await ws.accept()
        await ws.close(code=4003, reason="Terminal disabled in configuration")
        return

    if not await _increment_sessions():
        await ws.accept()
        await ws.close(
            code=4002,
            reason=f"Too many terminal sessions (max {MAX_CONCURRENT_SESSIONS})",
        )
        return

    await ws.accept()

    child_pid = -1
    master_fd = -1

    try:
        # pty.fork() returns (pid, master_fd).
        # In the child (pid==0), stdin/stdout/stderr are already connected
        # to the slave side of the pty.
        child_pid, master_fd = pty.fork()

        if child_pid == 0:
            # Child process — exec a login shell
            shell = os.environ.get("SHELL", "/bin/bash")
            os.execvpe(shell, [shell, "--login"], os.environ)
            os._exit(1)  # fallback if exec fails

        # Set default terminal size
        fcntl.ioctl(
            master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0)
        )

        last_input_time = time.monotonic()

        async def _read_pty():
            """Read pty output and send to WebSocket."""
            while True:
                try:
                    data = await asyncio.to_thread(_pty_read, master_fd)
                    if data is not None:
                        if data and ws.client_state == WebSocketState.CONNECTED:
                            await ws.send_text(data)
                    else:
                        # None means child exited
                        break
                except OSError:
                    break
                except Exception:
                    break

        reader_task = asyncio.create_task(_read_pty())

        try:
            while True:
                # Check idle timeout
                if time.monotonic() - last_input_time > IDLE_TIMEOUT_SECONDS:
                    logger.info("Terminal session idle timeout reached")
                    if ws.client_state == WebSocketState.CONNECTED:
                        await ws.close(
                            code=4001, reason="Session timed out (15 min idle)"
                        )
                    break

                try:
                    msg = await asyncio.wait_for(
                        ws.receive(), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    continue

                if msg.get("type") == "websocket.disconnect":
                    break

                text = msg.get("text")
                if text is None:
                    continue

                last_input_time = time.monotonic()

                # Handle resize messages: JSON {"type":"resize","cols":N,"rows":N}
                if text.startswith("{"):
                    try:
                        payload = json.loads(text)
                        if payload.get("type") == "resize":
                            rows = int(payload.get("rows", 24))
                            cols = int(payload.get("cols", 80))
                            fcntl.ioctl(
                                master_fd,
                                termios.TIOCSWINSZ,
                                struct.pack("HHHH", rows, cols, 0, 0),
                            )
                            continue
                    except (ValueError, KeyError):
                        pass  # Not a resize message, treat as input

                # Regular input — write to pty
                await asyncio.to_thread(
                    _pty_write, master_fd, text.encode("utf-8", errors="replace")
                )

        finally:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

    except Exception:
        logger.exception("Terminal WebSocket error")
    finally:
        if child_pid > 0:
            await asyncio.to_thread(_cleanup_child, child_pid, master_fd)
        await _decrement_sessions()


@router.get("/terminal")
async def terminal_page(request: Request):
    """Render the terminal page."""
    if not _is_terminal_enabled(request.app.state):
        from fastapi.responses import HTMLResponse

        return HTMLResponse("<h1>Terminal disabled</h1>", status_code=403)

    templates = request.app.state.templates
    return templates.TemplateResponse(request, "terminal.html", {})
