# Investigation: Test Instance Architecture & Isolation

**Ticket:** SMA-187
**Date:** 2026-02-27 (updated)

## Summary

The test instance infrastructure is already fully built and operational. All five investigation areas have existing, well-designed implementations. This report documents the current architecture and identifies minor gaps.

## Findings

### 1. Dashboard-Only Mode

**Status: Fully supported**

`create_app()` (`botfarm/dashboard.py:271`) accepts all dependencies as keyword-only parameters with sensible defaults:

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

**Can we run `create_app()` with a test DB and no callbacks?** Yes — this is exactly what the E2E tests already do (`tests/e2e/conftest.py:42`):
```python
app = create_app(db_path=seeded_db)
```

**What happens when callbacks are `None`?** The UI handles this gracefully:

- The supervisor controls template is guarded by `{% if has_callbacks %}` — buttons are not rendered when callbacks are `None`.
- API endpoints (`/api/pause`, `/api/resume`, `/api/update`) return `503` with a clear message: `"Pause not available (supervisor not connected)"`.
- Config editing (POST `/config`) returns `503` when `botfarm_config` is `None`.
- All read-only pages (index, history, usage, metrics, task detail) work normally.
- Tests confirm this behavior (`test_dashboard.py:2639-2651`).

**Conclusion:** Dashboard-only mode is production-ready. No changes needed.

### 2. Database Seeding Strategy

**Status: Comprehensive seed script exists (Option B implemented)**

`tests/seed_test_db.py` provides `seed_comprehensive_db()` — a deterministic, portable seed function covering:

| Data | Coverage |
|------|----------|
| **Slots** | 8 slots across 3 projects (`bot-farm`, `web-app`, `data-pipeline`), all 6 statuses: free, busy, failed, paused_manual, paused_limit, completed_pending_cleanup |
| **Tasks** | 10 tasks: completed (with full pipeline), failed (max iterations), in-progress (mid-implement), in-progress (mid-fix), paused (manual), paused (limit) |
| **Stage Runs** | ~30 stage runs across implement/review/fix/pr_checks/merge with varying iteration counts, context fills, costs, and token usage |
| **Usage** | 8 snapshots covering: low -> medium -> high -> at-limit -> extra-usage-active -> post-reset |
| **Queue** | 5 entries across 2 projects with priorities and blocker dependencies |
| **Dispatch State** | Running (not paused) |

**Minor gaps identified:**
- No `paused_manual` dispatch state (only running state is seeded)
- No tasks with `pr_url` populated (PR links column not exercised)

These are minor and can be added to the existing seed script without architectural changes (tracked in SMA-205).

### 3. Port & Process Isolation

**Status: Fully isolated**

| Aspect | Production | Test |
|--------|-----------|------|
| **Port** | 8420 (configurable via `config.yaml`) | 8421 (hardcoded in `tests/e2e/conftest.py:24`) |
| **Host** | `0.0.0.0` (configurable) | `127.0.0.1` (test only) |
| **DB Path** | Configured path (e.g., `~/.botfarm/botfarm.db`) | Temp directory (`tmp_path_factory.mktemp("e2e")`) |
| **Process** | Daemon thread in supervisor process | Daemon thread in pytest process |
| **Callbacks** | Connected to supervisor | None (inert) |

**Can they run on the same machine?** Yes — different ports, different databases, different processes. Test instance binds to `127.0.0.1` only.

**Accidental production DB connection?** Not possible — test fixture creates a fresh DB in a unique temp directory with no path overlap.

### 4. Configuration

**Status: No separate config file needed**

The test runner manages everything programmatically via `create_app()` arguments. For Playwright tests, the app is started in a pytest session fixture (`tests/e2e/conftest.py:36-72`) which handles startup, readiness polling (10s timeout), and graceful shutdown.

A `config.test.yaml` would only be needed to test the config editor with realistic values — can be added later as needed.

### 5. CI Considerations

**Status: Fully configured**

`.github/workflows/tests.yml` defines two CI jobs:

| Job | Command | Browser |
|-----|---------|---------|
| **Unit tests** | `python -m pytest tests/ -v -m "not playwright"` | None |
| **E2E tests** | `python -m pytest tests/e2e/ -v -m playwright --screenshot=only-on-failure --tracing=retain-on-failure` | Chromium (`playwright install chromium --with-deps`) |

- Test artifacts uploaded on failure (7-day retention)
- Marker `pytest.mark.playwright` configured in `pyproject.toml`
- Headless mode by default in CI

## Current Architecture

```
tests/
├── conftest.py                 # Shared unit test fixtures
├── seed_test_db.py             # Comprehensive DB seed module (reusable)
├── test_dashboard.py           # 44 unit/integration test classes
├── e2e/
│   ├── conftest.py             # E2E fixtures: seeded_db, live_server, page
│   ├── test_smoke.py           # Basic page load
│   ├── test_navigation.py      # Page navigation
│   ├── test_live_status.py     # Live status page
│   ├── test_task_history.py    # History page
│   ├── test_task_detail.py     # Task detail page
│   ├── test_usage_trends.py    # Usage trends
│   ├── test_metrics.py         # Metrics page
│   ├── test_log_viewer.py      # Log viewer
│   ├── test_realtime.py        # Real-time polling/SSE
│   ├── test_config.py          # Config editor
│   ├── test_identities.py      # Identities editor
│   └── scenarios/              # Scenario-based test data
```

Key design decisions already in place:
- `scope="session"` for DB and server — seed once, share across all Playwright tests
- Separate DB per Playwright session (no interference with unit tests)
- No production data, no production callbacks, no production config
- Fixed port 8421 (vs production 8420) for clarity

## Recommendation

**No new architecture is needed.** The existing implementation follows best practices:

1. **Isolation**: Complete separation via temp databases, different ports, no callbacks
2. **Determinism**: Seed script produces repeatable, comprehensive test data
3. **CI-ready**: Both unit and E2E test jobs configured with proper artifact collection
4. **Safety**: Dashboard-only mode is inert — no supervisor interaction possible

### Follow-up

Minor seed data gaps tracked in SMA-205:
1. Add paused dispatch state to seed data
2. Add tasks with `pr_url` populated
3. Consider random port allocation for concurrent test runs

### Impact on SMA-189

SMA-189 ("Set up Playwright infrastructure and test fixtures") appears to be already implemented. All five of its implementation tasks are complete. Recommend re-scoping to address only the minor gaps above, or closing as done.
