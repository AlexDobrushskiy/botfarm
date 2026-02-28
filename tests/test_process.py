"""Tests for botfarm.process — shared subprocess utilities."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

from botfarm.process import terminate_process_group


class TestTerminateProcessGroup:
    def test_already_exited_is_noop(self):
        """If process has already exited, no signals are sent."""
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        terminate_process_group(mock_proc)
        # No kill attempts

    @patch("botfarm.process.os.killpg")
    @patch("botfarm.process.os.getpgid", return_value=42)
    def test_sigterm_then_exit(self, mock_getpgid, mock_killpg):
        """SIGTERM is sent and process exits within grace period."""
        import signal

        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 42
        mock_proc.wait.return_value = -15

        terminate_process_group(mock_proc)

        mock_killpg.assert_called_once_with(42, signal.SIGTERM)
        mock_proc.wait.assert_called_once_with(timeout=5.0)

    @patch("botfarm.process.os.killpg")
    @patch("botfarm.process.os.getpgid", return_value=42)
    def test_sigterm_timeout_escalates_to_sigkill(self, mock_getpgid, mock_killpg):
        """If SIGTERM doesn't work within the grace period, SIGKILL is sent."""
        import signal

        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 42
        mock_proc.wait.side_effect = subprocess.TimeoutExpired(cmd="claude", timeout=5)

        terminate_process_group(mock_proc)

        assert mock_killpg.call_count == 2
        mock_killpg.assert_any_call(42, signal.SIGTERM)
        mock_killpg.assert_any_call(42, signal.SIGKILL)
