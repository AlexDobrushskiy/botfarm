# Dashboard

Optional web dashboard for monitoring and controlling the Botfarm supervisor. Runs as a background daemon thread inside the supervisor process.

## Stack

- **FastAPI** + **Jinja2** — server-rendered HTML, no SPA
- **PicoCSS 2.0** — lightweight CSS framework (CDN), built-in dark mode
- **htmx 2.0.4** — declarative AJAX polling and partial page updates (CDN)
- **sse-starlette** — Server-Sent Events for live log streaming
- **Vanilla JS** — modal dialogs, form handling (no frontend framework)

## Configuration

```yaml
dashboard:
  enabled: false       # default off
  host: "0.0.0.0"     # bind address
  port: 8420           # HTTP port
  terminal_enabled: false  # set to true to enable web terminal
```

## Architecture

```
Supervisor process
├── Main thread (polling, dispatch, timeouts)
└── Dashboard thread (daemon)
    └── Uvicorn → FastAPI app
        ├── Read-only SQLite access
        ├── Jinja2 template rendering
        └── Supervisor callbacks (pause/resume/stop/update)
```

**Key design decisions:**
- Dashboard reads the database but never writes — only the supervisor mutates state
- Supervisor passes typed callback functions for control actions (pause, resume, stop slot, update)
- htmx polls partials every 5s; expensive operations (usage API, git update check) are rate-limited to 60s
- No authentication — assumes network isolation (localhost or private network)
- Secrets are masked in config display (first 4 + last 4 chars)

## Module Structure

Dashboard code lives under `botfarm/dashboard/`:

| Module | Lines | Purpose |
|--------|-------|---------|
| `app.py` | ~185 | App factory, startup, Jinja2 config, callback injection |
| `state.py` | ~415 | SQLite read helpers, caching, rate limiting, state enrichment |
| `routes_main.py` | ~1021 | Page routes (index, history, tickets, usage, metrics, etc.) |
| `routes_partials.py` | ~247 | htmx polling endpoints (slots, queue, badges, banners) |
| `routes_api.py` | ~1050 | Control API + workflow CRUD |
| `routes_logs.py` | ~273 | Log viewer pages + SSE streaming |
| `routes_config.py` | ~554 | Runtime config/identity viewing and editing |
| `routes_terminal.py` | ~195 | WebSocket terminal (pty fork, resize, idle timeout) |
| `formatters.py` | ~319 | NDJSON parsing, log line formatting, pipeline state computation |

## Routes

### Pages

| Route | Description |
|-------|-------------|
| `GET /` | Live status — slots, queue, usage, supervisor controls |
| `GET /history` | Task history with filters and sorting |
| `GET /tickets` | Ticket browser with project/status filters |
| `GET /tickets/{id}` | Ticket detail with associated tasks |
| `GET /task/{id}` | Task detail — stages, events, pipeline visualization |
| `GET /usage` | Usage trends (24h / 7d / 30d time ranges) |
| `GET /metrics` | Aggregate metrics — success rate, costs, Codex review stats |
| `GET /workflow` | Pipeline editor — stages, loops, connections |
| `GET /config` | Configuration viewer/editor (View/Edit tabs) |
| `GET /identities` | Identity credentials management |
| `GET /health` | Preflight check results |
| `GET /task/{id}/logs[/{stage}]` | Log viewer with SSE streaming |
| `GET /terminal` | Web terminal (xterm.js + WebSocket + pty) |

### htmx Partials (polled every 5s)

| Route | Description |
|-------|-------------|
| `GET /partials/slots` | Slot table with context fill and pipeline state |
| `GET /partials/queue` | Queued tickets by project |
| `GET /partials/usage` | Usage widget |
| `GET /partials/supervisor-badge` | Supervisor health indicator |
| `GET /partials/supervisor-controls` | Pause/Resume/Stop buttons |
| `GET /partials/tracker-capacity` | Workspace capacity gauge |
| `GET /partials/history` | History table (paginated) |
| `GET /partials/tickets` | Ticket table (paginated) |
| `GET /partials/start-paused-banner` | Alert when starting paused |
| `GET /partials/update-banner` | Git update notification |
| `GET /partials/preflight-banner` | Critical check failure alert |
| `GET /partials/health-checks` | Full health check list |
| `GET /partials/health-badge` | Health status dot |

### Control API

| Route | Body | Description |
|-------|------|-------------|
| `POST /api/pause` | — | Pause dispatch globally |
| `POST /api/resume` | — | Resume dispatch |
| `POST /api/slot/stop` | `{project, slot_id, ticket_id?}` | Stop a specific slot (validates ticket not reassigned; 409 if slot reassigned) |
| `POST /api/slot/add` | `{project}` | Add a new slot to a running project |
| `POST /api/project/pause` | query: `?project=X&reason=Y` | Pause dispatch for one project |
| `POST /api/project/resume` | query: `?project=X` | Resume project dispatch |
| `POST /api/update` | — | Git pull + restart (requires `auto_restart=true`) |
| `POST /api/rerun-preflight` | — | Re-run preflight checks |

### Workflow CRUD

| Route | Description |
|-------|-------------|
| `GET /api/workflow/pipelines` | List all pipeline templates |
| `GET /api/workflow/pipelines/{id}` | Get pipeline with stages and loops |
| `POST /api/workflow/pipelines` | Create pipeline |
| `PATCH /api/workflow/pipelines/{id}` | Update pipeline metadata |
| `DELETE /api/workflow/pipelines/{id}` | Delete pipeline |
| `POST /api/workflow/pipelines/{id}/duplicate` | Duplicate pipeline |
| `POST /api/workflow/pipelines/{id}/stages` | Create stage |
| `PATCH /api/workflow/stages/{id}` | Update stage |
| `DELETE /api/workflow/stages/{id}` | Delete stage |
| `PUT /api/workflow/pipelines/{id}/stages/order` | Reorder stages |
| `POST /api/workflow/pipelines/{id}/loops` | Create loop |
| `PATCH /api/workflow/loops/{id}` | Update loop |
| `DELETE /api/workflow/loops/{id}` | Delete loop |

### Log Streaming

| Route | Description |
|-------|-------------|
| `GET /api/logs/{ticket_id}/{stage}/stream` | SSE stream of live log lines |
| `GET /api/logs/{ticket_id}/{stage}/content` | Download complete log file |

### Web Terminal

| Route | Description |
|-------|-------------|
| `WS /ws/terminal` | WebSocket endpoint — spawns a pty-backed shell session |

- Uses `pty.fork()` (Linux only) to create a pseudo-terminal per connection
- Bridges WebSocket messages to pty I/O via `asyncio.to_thread`
- Supports terminal resize via JSON messages: `{"type":"resize","cols":N,"rows":N}`
- Max 2 concurrent sessions; idle timeout after 15 minutes of no input
- Controlled by `dashboard.terminal_enabled` config flag (default `false` — opt-in)
- **Security:** no authentication — when exposing the dashboard externally, use a reverse proxy with auth (e.g. Caddy + basicauth, nginx + oauth2-proxy)

### Config & Identity API

| Route | Method | Body | Description |
|-------|--------|------|-------------|
| `POST /config` | POST | JSON | Update runtime-editable config values + YAML + DB sync |
| `GET /identities` | GET | — | Identity credentials page |
| `POST /identities` | POST | JSON | Update identity secrets (env + YAML) |
| `GET /api/preflight-results` | GET | — | Preflight check results (used by CLI `botfarm preflight`) |

## Templates

Templates live under `botfarm/dashboard/templates/`:

- `base.html` — master layout with nav, PicoCSS, htmx, inline styles, modal JS
- 13 page templates (`index.html`, `history.html`, `workflow.html`, etc.)
- 14 partial templates for htmx-refreshed components (`slots.html`, `queue.html`, etc.)

## Data Flow

1. **htmx polls** partial endpoints every 5s
2. **Route handler** opens read-only SQLite connection
3. **State enrichment** adds computed fields (context fill %, pipeline progress, Codex review status)
4. **Jinja2 renders** HTML fragment
5. **htmx swaps** fragment into the DOM (no full page reload)

For log streaming, the SSE endpoint tail-reads the active log file in a loop (`asyncio.sleep(0.5)`), parsing NDJSON lines into formatted HTML events. The stream closes when the stage completes.

## Supervisor Callbacks

The dashboard communicates with the supervisor via typed callback functions injected at startup in `app.py`:

```python
start_dashboard(
    config,
    db_path=...,
    on_pause=supervisor.request_pause,
    on_resume=supervisor.request_resume,
    on_update=supervisor.request_update,
    on_rerun_preflight=supervisor.request_rerun_preflight,
    on_stop_slot=supervisor.request_stop_slot,
    on_add_slot=supervisor.request_add_slot,
    get_preflight_results=supervisor.get_preflight_results,
    get_degraded=supervisor.get_degraded,
)
```

Callbacks are stored on `app.state` and called synchronously from route handlers. The supervisor queues the action for its next main-loop iteration (thread-safe via locks + `_wake_event`).

**Adding a new callback:** Add parameter to `start_dashboard()` and `_create_app()` in `app.py`, store on `app.state`, call from the relevant route handler.

## Form & Input Patterns

Existing patterns for user input in the dashboard. Follow these when adding new forms.

### Config Form (`/config`)
- Page has View/Edit tabs (JS `switchTab()` function)
- Form fields: `<input>`, `<select>`, number inputs
- JS collects values into nested JSON object
- `POST /config` with JSON body → returns HTML fragment (success/warning/error `<div>`)
- Structural changes (projects, slots) require restart and show a yellow warning banner
- Runtime changes apply immediately to in-memory config + YAML

### Identities Form (`/identities`)
- Text inputs for GitHub token, SSH key, Linear API key, git author
- Secrets masked in display (first 4 + last 4 chars)
- JS collects into `{coder: {field: value}, reviewer: {field: value}}`
- `POST /identities` → secrets written to `.env`, plain fields to `config.yaml`
- Response: HTML fragment with status message

### Stop Slot Modal (`/` index page)
- Native `<dialog>` element (`#stop-slot-modal`)
- Opened via `openStopModal(el)` — extracts `data-*` attributes from button
- Confirmed via `confirmStopSlot()` — `POST /api/slot/stop` with fetch
- Server validates slot state + ticket ID match (race condition protection)
- Toast notification on success/error

### Add Slot Button (`/` index page)
- One button per project: "+ Add Slot (ProjectName)"
- JS `confirm()` dialog → `POST /api/slot/add` with `{project}`
- All buttons disabled during request, re-enabled after
- Toast notification on result

### General Pattern
```javascript
// 1. Collect form data into JSON object
const data = { field1: el.value, field2: el2.value };
// 2. POST via fetch
const resp = await fetch('/api/endpoint', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(data),
});
// 3. Handle response (JSON for API, HTML fragment for forms)
if (resp.ok) showToast('Success', 'success');
else showToast('Error: ' + (await resp.json()).error, 'error');
```

## JavaScript Helpers

All JS is vanilla (no framework) and lives inline in templates — primarily `base.html` (~150 lines).

| Helper | Location | Purpose |
|--------|----------|---------|
| `showToast(message, type)` | `base.html` | 3-second notification popup (success/error/warning) |
| `timeago(isoString)` | `base.html` | Formats ISO date as "5m ago", "2h ago", "Mar 12" |
| `updateTimeagos()` | `base.html` | Updates all `[data-timeago]` elements (runs every 60s) |
| `openStopModal(el)` | `index.html` | Opens stop-slot dialog, populates from `data-*` attributes |
| `confirmStopSlot()` | `index.html` | Posts stop request, handles response |
| `addSlot(project)` | `index.html` | Confirm + post add-slot request |
| `switchTab(tab)` | `config.html` | Toggle View/Edit tabs |
| Connection recovery | `base.html` | Exponential backoff retry (5s→10s→20s→30s cap) on htmx errors |
| Countdown timers | `base.html` | Updates `[data-resume]` and `[data-reset]` countdowns every 10s |

## State Access

### Config (live, mutable)
```python
cfg = app.state.botfarm_config  # BotfarmConfig instance
projects = [p.name for p in cfg.projects]
poll_interval = cfg.bugtracker.poll_interval_seconds
```

### SQLite (read-only)
```python
conn = get_db(app)          # Opens read-only connection (returns None if unavailable)
state = read_state(app)     # Loads: slots, dispatch_paused, usage, queue, project pause states
```

### Rate-Limited External Calls
- **Usage API refresh:** 60s cache via `refresh_and_get_usage(app)` — falls back to DB snapshot
- **Git update check:** 60s cache via `check_commits_behind(app)`
- Implemented via `app.state._last_*` timestamps with locks

### State Enrichment (computed in route handlers)
- Context fill % — from latest task_event
- Pipeline progress per slot — completed/active/pending stages
- Codex review status — from last stage_run exit_subtype
- Capacity ratio — (issue_count / limit) × 100, color-coded

## Testing

Dashboard tests are split across files in `tests/`:
- `test_dashboard_index.py` — index page, slots, usage, queue
- `test_dashboard_history.py` — history/ticket browsing, filtering, pagination
- `test_dashboard_operations.py` — API endpoints, pause/resume, workflow CRUD
- `test_dashboard_workflow.py` — pipeline editor, stage/loop CRUD, validation
- `test_dashboard_terminal.py` — terminal page, WebSocket endpoint, session limits, config flag

Tests use FastAPI's `TestClient` with seeded SQLite databases. No Playwright/browser tests.
