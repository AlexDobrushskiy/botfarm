"""FastAPI application factory and startup/shutdown."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from botfarm.config import BotfarmConfig, DashboardConfig
from botfarm.devserver import DevServerManager

from .routes_api import router as api_router
from .routes_config import router as config_router
from .routes_devserver import router as devserver_router
from .routes_logs import router as logs_router
from .routes_main import router as main_router
from .routes_partials import router as partials_router
from .routes_projects import router as projects_router
from .routes_setup import router as setup_router
from .routes_terminal import router as terminal_router
from .state import STATIC_DIR, TEMPLATES_DIR, init_caches

logger = logging.getLogger(__name__)


def create_app(
    *,
    db_path: str | Path,
    workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
    state_file: str | Path | None = None,
    logs_dir: str | Path | None = None,
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    on_rerun_preflight: Callable[[], None] | None = None,
    on_stop_slot: Callable[[str, int], None] | None = None,
    on_add_slot: Callable[[str], None] | None = None,
    on_add_project: Callable[[str], None] | None = None,
    on_remove_project: Callable[[str], None] | None = None,
    get_preflight_results: Callable[[], list] | None = None,
    get_degraded: Callable[[], bool] | None = None,
    update_failed_event: threading.Event | None = None,
    get_update_failed_message: Callable[[], str] | None = None,
    git_env: dict[str, str] | None = None,
    auto_restart: bool = True,
    devserver_manager: DevServerManager | None = None,
) -> FastAPI:
    """Create the FastAPI dashboard application.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.
    workspace:
        Bugtracker workspace slug used for building ticket URLs.
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
    on_stop_slot:
        Callback invoked when the user clicks Stop on a slot. Takes
        ``(project, slot_id)`` arguments (e.g.
        ``supervisor.request_stop_slot``).
    on_add_slot:
        Callback invoked when the user requests adding a new slot. Takes
        ``(project,)`` argument (e.g. ``supervisor.request_add_slot``).
    on_add_project:
        Callback invoked when the dashboard registers a new project. Takes
        ``(project_name,)`` argument (e.g. ``supervisor.request_add_project``).
    on_remove_project:
        Callback invoked when the dashboard removes a project. Takes
        ``(project_name,)`` argument (e.g. ``supervisor.request_remove_project``).
    get_preflight_results:
        Callable that returns the latest list of preflight ``CheckResult``
        objects (e.g. ``supervisor.get_preflight_results``).
    get_degraded:
        Callable that returns whether the supervisor is in degraded mode.
    update_failed_event:
        Threading event set by the supervisor when an update fails.
        The banner endpoint checks this to reset the "Updating..." state.
    get_update_failed_message:
        Callable returning the error description when an update fails.
        Read by the banner endpoint after ``update_failed_event`` fires.
    devserver_manager:
        DevServerManager instance for controlling dev server processes.
    """
    app = FastAPI(title="Botfarm Dashboard", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Store paths on app state for route handlers
    app.state.db_path = Path(db_path).expanduser()
    app.state.workspace = workspace
    app.state.botfarm_config = botfarm_config
    app.state.auth_mode = botfarm_config.auth_mode if botfarm_config else "oauth"
    app.state.restart_required = False
    app.state.on_pause = on_pause
    app.state.on_resume = on_resume
    app.state.on_update = on_update
    app.state.on_rerun_preflight = on_rerun_preflight
    app.state.on_stop_slot = on_stop_slot
    app.state.on_add_slot = on_add_slot
    app.state.on_add_project = on_add_project
    app.state.on_remove_project = on_remove_project
    app.state.get_preflight_results = get_preflight_results
    app.state.get_degraded = get_degraded
    app.state.resume_requested_at = 0.0
    app.state.update_in_progress = False
    app.state.update_failed_event = update_failed_event
    app.state.get_update_failed_message = get_update_failed_message
    app.state.auto_restart = auto_restart
    app.state.logs_dir = Path(logs_dir).expanduser() if logs_dir else None
    app.state.git_env = git_env
    app.state.devserver_manager = devserver_manager
    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # Expose config flags to all templates via Jinja2 globals
    terminal_on = False
    if botfarm_config and hasattr(botfarm_config, "dashboard"):
        terminal_on = getattr(botfarm_config.dashboard, "terminal_enabled", False)
    app.state.templates.env.globals["terminal_enabled"] = terminal_on

    # Initialise per-app rate-limit caches (isolated per app instance)
    init_caches(app)

    # Seed the available_models table if empty (runs once at startup, not on
    # every GET /api/models request).  Only attempt when the DB file already
    # exists — we must not create it here (the supervisor owns creation).
    _db_file = Path(db_path).expanduser()
    if _db_file.exists():
        try:
            from botfarm.db import init_db
            from botfarm.models import ensure_seed_data
            _seed_conn = init_db(db_path)
            try:
                ensure_seed_data(_seed_conn)
            finally:
                _seed_conn.close()
        except Exception:
            logger.debug("Could not seed available_models at startup", exc_info=True)

    # Include all route modules
    app.include_router(main_router)
    app.include_router(partials_router)
    app.include_router(api_router)
    app.include_router(config_router)
    app.include_router(projects_router)
    app.include_router(logs_router)
    app.include_router(devserver_router)
    app.include_router(setup_router)
    app.include_router(terminal_router)

    return app


def start_dashboard(
    config: DashboardConfig,
    *,
    db_path: str | Path,
    workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
    state_file: str | Path | None = None,
    logs_dir: str | Path | None = None,
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    on_rerun_preflight: Callable[[], None] | None = None,
    on_stop_slot: Callable[[str, int], None] | None = None,
    on_add_slot: Callable[[str], None] | None = None,
    on_add_project: Callable[[str], None] | None = None,
    on_remove_project: Callable[[str], None] | None = None,
    get_preflight_results: Callable[[], list] | None = None,
    get_degraded: Callable[[], bool] | None = None,
    update_failed_event: threading.Event | None = None,
    get_update_failed_message: Callable[[], str] | None = None,
    git_env: dict[str, str] | None = None,
    auto_restart: bool = True,
    devserver_manager: DevServerManager | None = None,
) -> threading.Thread | None:
    """Start the dashboard server in a background daemon thread.

    Returns the thread if started, or None if the dashboard is disabled.
    """
    if not config.enabled:
        return None

    app = create_app(
        db_path=db_path,
        workspace=workspace,
        botfarm_config=botfarm_config,
        logs_dir=logs_dir,
        on_pause=on_pause,
        on_resume=on_resume,
        on_update=on_update,
        on_rerun_preflight=on_rerun_preflight,
        on_stop_slot=on_stop_slot,
        on_add_slot=on_add_slot,
        on_add_project=on_add_project,
        on_remove_project=on_remove_project,
        get_preflight_results=get_preflight_results,
        get_degraded=get_degraded,
        update_failed_event=update_failed_event,
        get_update_failed_message=get_update_failed_message,
        git_env=git_env,
        auto_restart=auto_restart,
        devserver_manager=devserver_manager,
    )

    def _run():
        import uvicorn
        uvicorn.run(
            app,
            host=config.host,
            port=config.port,
            log_level="warning",
            loop="auto",
        )

    thread = threading.Thread(target=_run, daemon=True, name="dashboard")
    thread.start()
    logger.info("Dashboard started on http://%s:%d", config.host, config.port)
    return thread
