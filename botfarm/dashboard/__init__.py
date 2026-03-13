"""Web dashboard for botfarm — FastAPI + Jinja2 + htmx.

Serves a lightweight server-rendered dashboard that auto-refreshes via htmx
polling. Designed to run inside the supervisor process as a background thread.
"""

from .app import create_app, start_dashboard
from .formatters import build_pipeline_state, format_codex_ndjson_line, format_ndjson_line

# Re-export names that tests and external code patch at botfarm.dashboard.*
from botfarm.git_update import commits_behind  # noqa: F401

__all__ = [
    "build_pipeline_state",
    "create_app",
    "format_codex_ndjson_line",
    "format_ndjson_line",
    "start_dashboard",
]
