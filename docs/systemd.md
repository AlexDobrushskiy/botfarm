# Running Botfarm as a systemd Service

Botfarm's primary deployment model is a headless Linux VM where it runs unattended. The `install-service` and `uninstall-service` CLI commands manage a **systemd user service** that starts the supervisor automatically and restarts it after crashes.

## Prerequisites

- Linux with systemd and user session support
- Botfarm installed (`pip install -e .`) with the `botfarm` executable on PATH
- A working `~/.botfarm/config.yaml` (run `botfarm init` to generate one)
- Environment variables set in `~/.botfarm/.env` (at minimum `LINEAR_API_KEY`)

## Installing the Service

```bash
botfarm install-service
```

This command:

1. Locates the `botfarm` binary on your PATH
2. Generates a systemd unit file
3. Writes it to `~/.config/systemd/user/botfarm.service`
4. Runs `systemctl --user daemon-reload`
5. Enables the service (so it starts on login/boot)

The generated unit file is previewed in the terminal before being written.

### Options

| Flag | Description |
|---|---|
| `--config <path>` | Config file path to pass to the service. Omit to use the default `~/.botfarm/config.yaml`. |
| `--working-dir <path>` | Working directory for the service. Defaults to the current directory. |
| `--env-file <path>` | Environment file(s) to load. May be repeated (e.g. `--env-file ~/.botfarm/.env --env-file ~/.botfarm/secrets.env`). |

### Example with Options

```bash
botfarm install-service \
  --config ~/.botfarm/config.yaml \
  --working-dir ~/code/my-project \
  --env-file ~/.botfarm/.env
```

## What the Unit File Looks Like

The generated `~/.config/systemd/user/botfarm.service` looks like:

```ini
[Unit]
Description=Botfarm Supervisor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/home/user/.local/bin/botfarm run
WorkingDirectory=/home/user/code/my-project
Restart=on-failure
RestartSec=5
Environment=PATH=/home/user/.local/bin:/usr/local/bin:/usr/bin:/bin
EnvironmentFile=-/home/user/.botfarm/.env

[Install]
WantedBy=default.target
```

Key details:

- **`Restart=on-failure`** — systemd restarts the supervisor on crashes (non-zero exit, SIGKILL, etc.) but does **not** restart on clean stops (exit 0, SIGTERM, SIGINT). This means `systemctl --user stop botfarm` cleanly stops it without a restart loop.
- **`RestartSec=5`** — waits 5 seconds before restarting after a crash.
- **`Environment=PATH=...`** — captures your current PATH at install time so that child processes (claude, codex, gh) can find their binaries. If you install new tools or change your PATH, re-run `botfarm install-service` to update the unit.
- **`EnvironmentFile=-`** — the `-` prefix means systemd won't fail if the file is missing. Variables from this file (like `LINEAR_API_KEY`) are passed to the botfarm process.

## Starting and Stopping the Service

After installation:

```bash
# Start the service
systemctl --user start botfarm

# Stop the service
systemctl --user stop botfarm

# Restart the service
systemctl --user restart botfarm

# Check service status
systemctl --user status botfarm
```

## Auto-Start on Boot (linger)

By default, systemd user services only run while the user has an active login session. For a headless VM where botfarm should start on boot — even before you SSH in — enable **loginctl linger**:

```bash
loginctl enable-linger
```

This tells systemd to start your user services at boot time, regardless of whether you're logged in. To disable:

```bash
loginctl disable-linger
```

## Viewing Logs

### journalctl (systemd logs)

```bash
# Follow live logs
journalctl --user -u botfarm -f

# View recent logs (last 100 lines)
journalctl --user -u botfarm -n 100

# View logs since last boot
journalctl --user -u botfarm -b

# View logs from a specific time
journalctl --user -u botfarm --since "2025-03-01 10:00:00"
```

### Application log files

Botfarm also writes its own log files under `~/.botfarm/logs/` (see [runtime-files.md](runtime-files.md) for the full layout):

- `supervisor.log` — main supervisor log (rotated)
- `<TICKET-ID>/worker.log` — per-ticket worker log
- `<TICKET-ID>/<stage>-<timestamp>.log` — raw Claude subprocess output

The journalctl output and the supervisor log contain the same information. The per-ticket logs are only in the files, not in journalctl.

## Troubleshooting

### Service won't start

```bash
# Check status and recent logs
systemctl --user status botfarm

# Check for configuration errors
journalctl --user -u botfarm -n 50
```

Common causes:
- Missing `~/.botfarm/config.yaml` — run `botfarm init`
- Missing environment variables — check `~/.botfarm/.env`
- `botfarm` binary not found — the PATH in the unit file may be stale; re-run `botfarm install-service`

### Stale unit file warning

Botfarm checks the installed unit file at startup and warns if it's stale. This happens when:

- The unit is missing the `Environment=PATH=` directive (child processes can't find binaries)
- The unit contains `--no-auto-restart` (prevents dashboard-triggered updates)

Fix by re-running:

```bash
botfarm install-service
```

### Service restarts unexpectedly

`Restart=on-failure` means systemd only restarts after crashes (non-zero exit). If the supervisor is crashing:

1. Check `journalctl --user -u botfarm -n 200` for the crash reason
2. Check `~/.botfarm/logs/supervisor.log` for detailed error messages
3. Fix the underlying issue (bad config, network problems, etc.)

### Can't connect to systemd user bus via SSH

If `systemctl --user` fails over SSH with "Failed to connect to bus", ensure your SSH session has the right environment:

```bash
export XDG_RUNTIME_DIR=/run/user/$(id -u)
```

Some SSH configurations don't set this automatically. You can add it to your `~/.bashrc`.

## Running with nohup (Alternative to systemd)

If you prefer `nohup` over systemd, be aware that non-login shells don't source `~/.bashrc`, so `~/.local/bin` (where Claude Code is typically installed) may not be in PATH. Botfarm automatically prepends `~/.local/bin` to PATH at startup, but if Claude Code is installed elsewhere, set PATH explicitly:

```bash
nohup botfarm run >> ~/.botfarm/logs/nohup.log 2>&1 &
```

If Claude Code is in a non-standard location:

```bash
PATH=/custom/path:$PATH nohup botfarm run >> ~/.botfarm/logs/nohup.log 2>&1 &
```

The systemd service is the recommended approach — it handles PATH, restarts on crashes, and integrates with `journalctl` for log viewing.

## Don't Run Both

Do **not** run `botfarm run` manually while the systemd service is active. This would create two competing supervisor instances polling the same Linear tickets and dispatching duplicate work. Either:

- Use the systemd service (`systemctl --user start botfarm`), **or**
- Run manually in the foreground (`botfarm run`)

To check if the service is running:

```bash
systemctl --user is-active botfarm
```

## Uninstalling the Service

```bash
botfarm uninstall-service
```

This command:

1. Stops the service (if running)
2. Disables it (removes the auto-start link)
3. Deletes `~/.config/systemd/user/botfarm.service`
4. Runs `systemctl --user daemon-reload`

## Dashboard Access

If you have the dashboard enabled in your config, it will be accessible at `http://<vm-ip>:8420` (default port) while the service is running. The dashboard runs as a background thread inside the supervisor process — no separate service needed.

To change the port or bind address, update your `config.yaml`:

```yaml
dashboard:
  enabled: true
  host: 0.0.0.0
  port: 8420
```
