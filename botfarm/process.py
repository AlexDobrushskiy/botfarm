"""Shared subprocess utilities."""

from __future__ import annotations

import os
import signal
import subprocess


def terminate_process_group(proc: subprocess.Popen, graceful_timeout: float = 5.0) -> None:
    """Send SIGTERM then SIGKILL to the process group headed by *proc*.

    Because we launch subprocesses with ``start_new_session=True``, the PID
    is the process group leader.  Sending signals to the *group* ensures
    child processes that inherited the stdout pipe are also terminated.
    """
    if proc.poll() is not None:
        return  # already exited
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
    except OSError:
        return
    try:
        proc.wait(timeout=graceful_timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except OSError:
            pass
