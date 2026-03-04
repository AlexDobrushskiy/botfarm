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

| Module | Purpose |
|--------|---------|
| `dashboard.py` | App factory, startup, Jinja2 config |
| `state.py` | Shared state reads, caching, rate limiting |
| `routes_main.py` | Page routes (index, history, tickets, usage, metrics, etc.) |
| `routes_partials.py` | htmx polling endpoints (slots, queue, badges, banners) |
| `routes_api.py` | Control API + workflow CRUD + cleanup operations |
| `routes_logs.py` | Log viewer pages + SSE streaming |
| `routes_config.py` | Runtime config viewing and editing |

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
| `GET /config` | Configuration viewer/editor |
| `GET /cleanup` | Bulk ticket archival/deletion interface |
| `GET /health` | Preflight check results |
| `GET /task/{id}/logs[/{stage}]` | Log viewer with SSE streaming |

### htmx Partials (polled every 5s)

| Route | Description |
|-------|-------------|
| `GET /partials/slots` | Slot table with context fill and pipeline state |
| `GET /partials/queue` | Queued tickets by project |
| `GET /partials/usage` | Usage widget |
| `GET /partials/supervisor-badge` | Supervisor health indicator |
| `GET /partials/supervisor-controls` | Pause/Resume/Stop buttons |
| `GET /partials/linear-capacity` | Workspace capacity gauge |
| `GET /partials/history` | History table (paginated) |
| `GET /partials/tickets` | Ticket table (paginated) |
| `GET /partials/start-paused-banner` | Alert when starting paused |
| `GET /partials/update-banner` | Git update notification |
| `GET /partials/preflight-banner` | Critical check failure alert |
| `GET /partials/health-checks` | Full health check list |
| `GET /partials/health-badge` | Health status dot |

### Control API

| Route | Description |
|-------|-------------|
| `POST /api/pause` | Pause dispatch globally |
| `POST /api/resume` | Resume dispatch |
| `POST /api/slot/stop` | Stop a specific slot (validates ticket not reassigned) |
| `POST /api/project/pause` | Pause dispatch for one project |
| `POST /api/project/resume` | Resume project dispatch |
| `POST /api/update` | Git pull + restart (requires `auto_restart=true`) |
| `POST /api/rerun-preflight` | Re-run preflight checks |

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

### Cleanup API

| Route | Description |
|-------|-------------|
| `GET /api/cleanup/preview` | Preview candidate issues for archival |
| `POST /api/cleanup/execute` | Run bulk cleanup |
| `POST /api/cleanup/undo/{batch_id}` | Undo archive batch |
| `GET /api/cleanup/batch/{batch_id}` | Get batch details |

### Log Streaming

| Route | Description |
|-------|-------------|
| `GET /api/logs/{ticket_id}/{stage}/stream` | SSE stream of live log lines |
| `GET /api/logs/{ticket_id}/{stage}/content` | Download complete log file |

### Config API

| Route | Description |
|-------|-------------|
| `POST /config` | Update runtime-editable config values |

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

## Testing

Dashboard tests are split across 4 files in `tests/`:
- `test_dashboard_index.py` — index page, slots, usage, queue
- `test_dashboard_history.py` — history/ticket browsing, filtering, pagination
- `test_dashboard_operations.py` — API endpoints, pause/resume, cleanup, workflow CRUD
- `test_dashboard_workflow.py` — pipeline editor, stage/loop CRUD, validation

Tests use FastAPI's `TestClient` with seeded SQLite databases. No Playwright/browser tests.
