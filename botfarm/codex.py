"""Codex reviewer integration utilities."""

from __future__ import annotations

import shutil


def check_codex_available() -> bool:
    """Return True if the ``codex`` binary is found on PATH."""
    return shutil.which("codex") is not None
