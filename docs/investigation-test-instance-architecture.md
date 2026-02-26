# Investigation: Test Instance Architecture & Isolation

**Ticket:** SMA-187
**Date:** 2026-02-26

## Summary

The botfarm dashboard is already well-architected for test isolation. The `create_app()` factory accepts all dependencies as parameters, callbacks default to `None` (disabling supervisor actions), and SQLite with `tmp_path` provides trivial database isolation. No significant architectural changes are needed — just a seed script and CI plumbing.

## Findings

### 1. Dashboard-Only Mode

**Can we run `create_app()` with a test DB and no callbacks?** Yes — this already works and is how the existing `test_dashboard.py` tests operate.

`create_app()` signature (`botfarm/dashboard.py:271`):

```python
def create_app(
    *,
    db_path: str | Path,              # Required — path to SQLite DB
    linear_workspace: str = "",        # For building ticket URLs
    botfarm_config: BotfarmConfig | None = None,  # None disables config editing
    state_file: str | Path | None = None,         # Deprecated, ignored
    logs_dir: str | Path | None = None,           # None disables log viewer
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    update_failed_event: threading.Event | None = None,
    git_env: dict[str, str] | None = None,
) -> FastAPI:
```

**What happens when callbacks are `None`?** The UI handles this gracefully:

- The supervisor controls template (`partials/supervisor_controls.html`) is guarded by `{% if has_callbacks %}` — when callbacks are `None`, Pause/Resume/Update buttons are simply not rendered.
- API endpoints (`/api/pause`, `/api/resume`, `/api/update`) return `503` with a clear error message: `"Pause not available (supervisor not connected)"`.
- Config editing (POST `/config`) similarly returns `503` when `botfarm_config` is `None`.
- All read-only pages (index, history, usage, metrics, task detail) work normally.

**Conclusion:** Dashboard-only mode is production-ready. No changes needed.

### 2. Database Seeding Strategy

**Current schema:** 13 migrations, 8 core tables:
- `slots` — runtime slot state (status: free/busy/failed/paused_manual)
- `tasks` — ticket processing history (status: pending/completed/failed)
- `stage_runs` — per-stage execution metrics (stages: implement/review/fix/pr_checks/merge)
- `usage_snapshots` — API usage polling data (utilization_5h, utilization_7d, extra usage)
- `task_events` — audit trail
- `dispatch_state` — single-row pause state + supervisor heartbeat
- `queue_entries` — Linear Todo queue snapshots
- `schema_version` — migration tracking

**Existing test fixtures** (`tests/test_dashboard.py`):
- `db_file` fixture creates a DB with 2 slots, 2 tasks, 2 stage_runs, 1 usage snapshot, and 3 task events.
- Helper functions `_seed_slot()` and `_seed_queue_entry()` exist for ergonomic seeding.
- Uses `init_db()` + the `botfarm.db` helper functions — handles all migrations automatically.

**Minimum dataset to exercise all dashboard pages:**

| Page | Required Data |
|------|--------------|
| Index (/) | slots (all statuses), dispatch_state, usage_snapshot, queue_entries |
| History (/history) | tasks (completed, failed, pending), multiple projects |
| Usage (/usage) | Multiple usage_snapshots over time |
| Metrics (/metrics) | stage_runs with varied stages/durations/costs |
| Task Detail (/task/N) | task + stage_runs + task_events |
| Config (/config) | BotfarmConfig object (optional) |
| Log Viewer (/logs) | logs_dir with sample files (optional) |

**Recommendation:** Option B — purpose-built seed script. Reasons:
- Deterministic and portable (no dependency on production data)
- Can be checked into the repo and reused across CI/local/Playwright
- The existing fixture pattern is a proven template to extend
- Seed script should live at `tests/seed_test_db.py` as a reusable module importable by both pytest fixtures and a standalone CLI

### 3. Port & Process Isolation

**Production port:** 8420 (configured in `DashboardConfig`).

**For Playwright tests:** No port binding needed. FastAPI's `TestClient` uses HTTPX transport internally — it calls the ASGI app directly without opening a network socket. All existing dashboard tests already use this approach.

However, Playwright requires a real browser connecting to a real HTTP endpoint. Two options:

- **Option A: Uvicorn on an ephemeral port.** Start `uvicorn.run(app, port=0)` in a thread (port=0 lets the OS pick a free port). Playwright connects to that port.
- **Option B: Use `pytest-playwright` with a pytest fixture** that starts the app on a fixed test port (e.g., 8421) and tears it down after. Simpler, and port conflicts are unlikely in CI.

**Preventing accidental production DB access:** The seed script creates a fresh DB in `tmp_path`. The `create_app()` call explicitly receives this path. There is no global DB path that could accidentally resolve to production.

**Recommendation:** Option B (fixed test port 8421) for simplicity. The test fixture pattern:

```python
@pytest.fixture(scope="session")
def dashboard_server(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("data") / "test.db"
    seed_comprehensive_db(db_path)  # From seed script
    app = create_app(db_path=db_path)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8421))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    yield "http://127.0.0.1:8421"
    server.should_exit = True
```

### 4. Configuration

**Do we need a separate `config.test.yaml`?** No. The test instance is fully configured programmatically through `create_app()` parameters. There is no need to load a config file at all.

For Playwright tests specifically:
- `create_app(db_path=..., linear_workspace="test-workspace")` — all that's needed
- `botfarm_config=None` — disables config editing (safe)
- `logs_dir=None` — disables log viewer (unless we want to test it)
- No callbacks — disables supervisor controls

If we later want to test config editing or log viewing, we can pass a test `BotfarmConfig` or a temp `logs_dir` to specific test fixtures.

### 5. CI Considerations

**Current CI** (`.github/workflows/tests.yml`):
- Python 3.12, pip install, `pytest tests/ -v`
- No browser installation, no Playwright

**What's needed for Playwright tests:**

1. **Browser installation:** `playwright install chromium --with-deps` (~150MB download, cached by GH Actions)
2. **Python package:** `pytest-playwright` added to `requirements.txt`
3. **Separate test job or marker:** Playwright tests are slower (~5-10s per test vs <0.1s for unit tests). Recommended approach:
   - Mark Playwright tests with `@pytest.mark.playwright`
   - Run unit tests and Playwright tests in separate CI jobs (can run in parallel)
   - Or use `-m "not playwright"` for fast feedback, full suite on PR
4. **Headless mode:** Default for `pytest-playwright` in CI — no config needed
5. **Artifacts on failure:** `pytest-playwright` supports `--screenshot=only-on-failure` and `--tracing=retain-on-failure` for debugging
6. **Server startup in CI:** The pytest fixture handles this — no separate process management needed

**Estimated CI impact:** +30-60s for browser install (cached after first run), +10-30s per Playwright test file.

### 6. Recommended Architecture

```
tests/
├── conftest.py                     # Shared fixtures (if any)
├── seed_test_db.py                 # Comprehensive DB seed module
├── test_dashboard.py               # Existing unit tests (unchanged)
├── test_dashboard_playwright.py    # NEW: Playwright E2E tests
└── ...

# Playwright test structure:
@pytest.fixture(scope="session")
def seeded_db(tmp_path_factory) -> Path:
    """Create and seed a comprehensive test database."""
    ...

@pytest.fixture(scope="session")
def live_server(seeded_db) -> str:
    """Start dashboard on port 8421, return base URL."""
    ...

@pytest.fixture()
def page(live_server, browser):
    """Playwright page pointed at the test dashboard."""
    ...
```

**Key design decisions:**
- `scope="session"` for DB and server — seed once, share across all Playwright tests
- Separate DB per Playwright session (no interference with unit tests)
- No production data, no production callbacks, no production config
- Fixed port 8421 (vs production 8420) for clarity

## Follow-up Tickets Needed

1. **Comprehensive DB seed script** — Create `tests/seed_test_db.py` covering all UI states
2. **Playwright test infrastructure** — Add `pytest-playwright`, conftest fixtures, CI workflow changes
3. **Initial Playwright test suite** — E2E tests for all dashboard pages and interactions
