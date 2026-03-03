"""FastAPI application factory and startup/shutdown."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from botfarm.config import BotfarmConfig, DashboardConfig

from .routes_api import router as api_router
from .routes_config import router as config_router
from .routes_logs import router as logs_router
from .routes_main import router as main_router
from .routes_partials import router as partials_router
from .state import elapsed, format_duration, reset_caches

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def create_app(
    *,
    db_path: str | Path,
    linear_workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
    state_file: str | Path | None = None,
    logs_dir: str | Path | None = None,
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    on_rerun_preflight: Callable[[], None] | None = None,
    get_preflight_results: Callable[[], list] | None = None,
    get_degraded: Callable[[], bool] | None = None,
    update_failed_event: threading.Event | None = None,
    git_env: dict[str, str] | None = None,
    auto_restart: bool = True,
) -> FastAPI:
    """Create the FastAPI dashboard application.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.
    linear_workspace:
        Linear workspace slug used for building ticket URLs.
    botfarm_config:
        Live BotfarmConfig object for runtime editing. If ``None``, the
        config page is disabled.
    state_file:
        Deprecated. Ignored. Kept only for backward compatibility during
        the transition period.
    logs_dir:
        Base directory for per-ticket log files (e.g. ``~/.botfarm/logs``).
        When set, the log viewer feature is enabled.
    on_pause:
        Callback invoked when the user clicks Pause. Should be a callable
        with no arguments (e.g. ``supervisor.request_pause``).
    on_resume:
        Callback invoked when the user clicks Resume. Should be a callable
        with no arguments (e.g. ``supervisor.request_resume``).
    on_update:
        Callback invoked when the user clicks Update & Restart. Should be
        a callable with no arguments (e.g. ``supervisor.request_update``).
    on_rerun_preflight:
        Callback invoked when the user triggers a manual preflight re-run
        from the dashboard (e.g. ``supervisor.request_rerun_preflight``).
    get_preflight_results:
        Callable that returns the latest list of preflight ``CheckResult``
        objects (e.g. ``supervisor.get_preflight_results``).
    get_degraded:
        Callable that returns whether the supervisor is in degraded mode.
    update_failed_event:
        Threading event set by the supervisor when an update fails.
        The banner endpoint checks this to reset the "Updating..." state.
    """
    app = FastAPI(title="Botfarm Dashboard", docs_url=None, redoc_url=None)

    # Reset module-level caches so each app instance starts clean
    reset_caches()

    # Store paths on app state for route handlers
    app.state.db_path = Path(db_path).expanduser()
    app.state.linear_workspace = linear_workspace
    app.state.botfarm_config = botfarm_config
    app.state.restart_required = False
    app.state.on_pause = on_pause
    app.state.on_resume = on_resume
    app.state.on_update = on_update
    app.state.on_rerun_preflight = on_rerun_preflight
    app.state.get_preflight_results = get_preflight_results
    app.state.get_degraded = get_degraded
    app.state.update_in_progress = False
    app.state.update_failed_event = update_failed_event
    app.state.auto_restart = auto_restart
    app.state.logs_dir = Path(logs_dir).expanduser() if logs_dir else None
    app.state.git_env = git_env

    # Make helpers available to templates via middleware
    @app.middleware("http")
    async def add_template_globals(request: Request, call_next):
        request.state.elapsed = elapsed
        request.state.format_duration = format_duration
        return await call_next(request)

    # Include all route modules
    app.include_router(main_router)
    app.include_router(partials_router)
    app.include_router(api_router)
    app.include_router(config_router)
    app.include_router(logs_router)

    return app


def start_dashboard(
    config: DashboardConfig,
    *,
    db_path: str | Path,
    linear_workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
    state_file: str | Path | None = None,
    logs_dir: str | Path | None = None,
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    on_rerun_preflight: Callable[[], None] | None = None,
    get_preflight_results: Callable[[], list] | None = None,
    get_degraded: Callable[[], bool] | None = None,
    update_failed_event: threading.Event | None = None,
    git_env: dict[str, str] | None = None,
    auto_restart: bool = True,
) -> threading.Thread | None:
    """Start the dashboard server in a background daemon thread.

    Returns the thread if started, or None if the dashboard is disabled.
    """
    if not config.enabled:
        return None

    app = create_app(
        db_path=db_path,
        linear_workspace=linear_workspace,
        botfarm_config=botfarm_config,
        logs_dir=logs_dir,
        on_pause=on_pause,
        on_resume=on_resume,
        on_update=on_update,
        on_rerun_preflight=on_rerun_preflight,
        get_preflight_results=get_preflight_results,
        get_degraded=get_degraded,
        update_failed_event=update_failed_event,
        git_env=git_env,
        auto_restart=auto_restart,
    )

    def _run():
        import uvicorn
        uvicorn.run(
            app,
            host=config.host,
            port=config.port,
            log_level="warning",
        )

    thread = threading.Thread(target=_run, daemon=True, name="dashboard")
    thread.start()
    logger.info("Dashboard started on http://%s:%d", config.host, config.port)
    return thread
