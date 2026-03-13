# Botfarm

**[botfarm.run](https://botfarm.run)**

Autonomous Linear ticket dispatcher for Claude Code agents.

Botfarm polls your Linear board for "Todo" tickets, dispatches them to Claude Code agent workers running in parallel git worktrees, and manages the full pipeline: implement → review → fix → PR checks → merge.

## Prerequisites

### System Requirements

- **Python 3.12+**
- **git**

### Claude Code

Install using the standalone installer:

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

> **Note:** Use `| bash`, not `| sh` — the latter fails on Ubuntu.

After installation, add `~/.local/bin` to your PATH if it isn't already:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Node.js is **not** required.

### Claude Code Linear Plugin

The Linear plugin is required for workers to manage tickets via Linear MCP.

1. Open Claude Code: `claude`
2. Type `/plugins`
3. Select "Discover" and search for "linear"
4. Install the Linear plugin

### GitHub CLI

Install the GitHub CLI (`gh`) following the [official instructions](https://github.com/cli/cli#installation), then authenticate:

```bash
gh auth login
```

Select the **SSH** protocol when prompted. This automatically generates and uploads an SSH key — no manual `ssh-keygen` needed.

### Linear API Key

Create a personal API key at [Linear Settings → API](https://linear.app/settings/api). You'll add this to `~/.botfarm/.env` during setup.

## Headless Server Setup

When running on a headless server (no browser), authentication requires extra steps.

### Claude Code Auth

Claude Code uses a device code flow that works headless:

1. Run `claude` on the server
2. It displays a URL and a code
3. Open the URL in a browser on any machine, paste the code
4. Authentication completes on the server

### Linear Plugin Auth

The Linear plugin uses OAuth which requires a browser callback. Use an SSH tunnel:

1. Start the SSH tunnel from your local machine to the server:
   ```bash
   ssh -L 3000:localhost:3000 user@server
   ```
2. On the server, open Claude Code and install the Linear plugin (`/plugins` → discover → "linear")
3. When the OAuth flow starts, open the authorization URL in your local browser
4. The callback redirects to `localhost:3000`, which tunnels back to the server

### GitHub CLI Auth

Use the browser-based device code flow (recommended over token-based auth):

```bash
gh auth login
```

1. Select **GitHub.com**
2. Select **SSH** protocol
3. Select **Login with a web browser**
4. It displays a one-time code — open the URL on any machine and enter it

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Configuration

Generate a starter config:

```bash
botfarm init
```

This creates `~/.botfarm/config.yaml`. Add your Linear API key to `~/.botfarm/.env`:

```
LINEAR_API_KEY=lin_api_xxxxxxxxxxxxxxxxxxxx
```

See [docs/configuration.md](docs/configuration.md) for the full config reference.

## Usage

```bash
botfarm run              # Start the supervisor
botfarm status           # Show current slot states
botfarm history          # Show recent task history
botfarm limits           # Show usage limit utilization
botfarm --help           # Full CLI help
```

## Testing

```bash
python -m pytest tests/ -v
```

## Documentation

- [Configuration Guide](docs/configuration.md) — full config reference with examples
- [Linear Workflow](docs/linear-workflow.md) — ticket creation, sizing, and workflow
- [Runtime Files](docs/runtime-files.md) — `~/.botfarm/` directory layout and logs
- [Database](docs/database.md) — SQLite schema and migrations
- [Dashboard](docs/dashboard.md) — optional web dashboard
