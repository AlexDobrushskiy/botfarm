"""Config and identity management routes."""

from __future__ import annotations

import html
import logging
import yaml
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from botfarm.config import (
    EDITABLE_FIELDS,
    apply_config_updates,
    sync_agent_config_to_db,
    validate_config_updates,
    validate_structural_config_updates,
    write_config_updates,
    write_structural_config_updates,
    write_yaml_atomic,
)
from botfarm.db import init_db

from .state import manual_pause_state, read_state, supervisor_status

logger = logging.getLogger(__name__)

router = APIRouter()


def _mask_secret(value: str) -> str:
    """Mask a secret string, showing first 4 + last 4 chars."""
    if not value:
        return ""
    if len(value) <= 8:
        return "****"
    return value[:4] + "****" + value[-4:]


def _full_config_values(app) -> dict:
    """Extract the full running config as a nested dict for display."""
    cfg = app.state.botfarm_config
    if cfg is None:
        return {}
    return {
        "projects": [
            {
                "name": p.name,
                "linear_team": p.linear_team,
                "linear_project": p.linear_project,
                "base_dir": p.base_dir,
                "worktree_prefix": p.worktree_prefix,
                "slots": list(p.slots),
            }
            for p in cfg.projects
        ],
        "linear": {
            "api_key": _mask_secret(cfg.linear.api_key),
            "workspace": cfg.linear.workspace,
            "poll_interval_seconds": cfg.linear.poll_interval_seconds,
            "exclude_tags": list(cfg.linear.exclude_tags),
            "todo_status": cfg.linear.todo_status,
            "in_progress_status": cfg.linear.in_progress_status,
            "done_status": cfg.linear.done_status,
            "in_review_status": cfg.linear.in_review_status,
            "failed_status": cfg.linear.failed_status,
            "comment_on_failure": cfg.linear.comment_on_failure,
            "comment_on_completion": cfg.linear.comment_on_completion,
            "comment_on_limit_pause": cfg.linear.comment_on_limit_pause,
        },
        "agents": {
            "max_review_iterations": cfg.agents.max_review_iterations,
            "max_ci_retries": cfg.agents.max_ci_retries,
            "timeout_minutes": dict(cfg.agents.timeout_minutes),
            "timeout_overrides": {
                label: dict(stages)
                for label, stages in cfg.agents.timeout_overrides.items()
            },
            "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
            "codex_reviewer_enabled": cfg.agents.codex_reviewer_enabled,
            "codex_reviewer_model": cfg.agents.codex_reviewer_model,
            "codex_reviewer_timeout_minutes": cfg.agents.codex_reviewer_timeout_minutes,
        },
        "usage_limits": {
            "enabled": cfg.usage_limits.enabled,
            "pause_five_hour_threshold": cfg.usage_limits.pause_five_hour_threshold,
            "pause_seven_day_threshold": cfg.usage_limits.pause_seven_day_threshold,
        },
        "notifications": {
            "webhook_url": _mask_secret(cfg.notifications.webhook_url),
            "webhook_format": cfg.notifications.webhook_format,
            "rate_limit_seconds": cfg.notifications.rate_limit_seconds,
        },
        "dashboard": {
            "enabled": cfg.dashboard.enabled,
            "host": cfg.dashboard.host,
            "port": cfg.dashboard.port,
        },
        "database": {
            "path": str(app.state.db_path),
        },
    }


def _config_values(app) -> dict:
    """Extract current editable config values as a nested dict."""
    cfg = app.state.botfarm_config
    if cfg is None:
        return {}
    return {
        "linear": {
            "poll_interval_seconds": cfg.linear.poll_interval_seconds,
            "comment_on_failure": cfg.linear.comment_on_failure,
            "comment_on_completion": cfg.linear.comment_on_completion,
            "comment_on_limit_pause": cfg.linear.comment_on_limit_pause,
        },
        "usage_limits": {
            "enabled": cfg.usage_limits.enabled,
            "pause_five_hour_threshold": cfg.usage_limits.pause_five_hour_threshold,
            "pause_seven_day_threshold": cfg.usage_limits.pause_seven_day_threshold,
        },
        "agents": {
            "max_review_iterations": cfg.agents.max_review_iterations,
            "max_ci_retries": cfg.agents.max_ci_retries,
            "timeout_minutes": dict(cfg.agents.timeout_minutes),
            "timeout_overrides": {
                label: dict(stages)
                for label, stages in cfg.agents.timeout_overrides.items()
            },
            "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
            "codex_reviewer_enabled": cfg.agents.codex_reviewer_enabled,
            "codex_reviewer_model": cfg.agents.codex_reviewer_model,
            "codex_reviewer_timeout_minutes": cfg.agents.codex_reviewer_timeout_minutes,
        },
        "notifications": {
            "webhook_url": cfg.notifications.webhook_url,
            "webhook_format": cfg.notifications.webhook_format,
            "rate_limit_seconds": cfg.notifications.rate_limit_seconds,
        },
        "projects": [
            {
                "name": p.name,
                "slots": list(p.slots),
                "linear_project": p.linear_project,
            }
            for p in cfg.projects
        ],
    }


@router.get("/config", response_class=HTMLResponse)
def config_page(request: Request):
    app = request.app
    templates = request.app.state.templates
    cfg = app.state.botfarm_config
    enabled = cfg is not None
    state = read_state(app)
    return templates.TemplateResponse("config.html", {
        "request": request,
        "config_enabled": enabled,
        "config_values": _config_values(app),
        "full_config_values": _full_config_values(app),
        "editable_fields": EDITABLE_FIELDS,
        "restart_required": app.state.restart_required,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.post("/config", response_class=HTMLResponse)
async def config_update(request: Request):
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            "Config editing is not available.</div>",
            status_code=400,
        )

    try:
        updates = await request.json()
    except Exception:
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            "Invalid JSON body.</div>",
            status_code=400,
        )

    if not isinstance(updates, dict):
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            "Request body must be a JSON object.</div>",
            status_code=400,
        )

    # Split into runtime-editable and structural updates
    structural_sections = {"notifications", "projects"}
    runtime_updates = {
        k: v for k, v in updates.items() if k not in structural_sections
    }
    structural_updates = {
        k: v for k, v in updates.items() if k in structural_sections
    }

    # Validate runtime updates
    all_errors: list[str] = []
    if runtime_updates:
        all_errors.extend(validate_config_updates(runtime_updates, cfg))

    # Validate structural updates
    if structural_updates:
        all_errors.extend(
            validate_structural_config_updates(structural_updates, cfg)
        )

    if all_errors:
        error_html = "".join(
            f"<li>{html.escape(e)}</li>" for e in all_errors
        )
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            f"<strong>Validation errors:</strong><ul>{error_html}</ul></div>",
            status_code=422,
        )

    config_path = Path(cfg.source_path) if cfg.source_path else None

    # Apply runtime updates to in-memory config + YAML
    if runtime_updates:
        apply_config_updates(cfg, runtime_updates)
        if config_path and config_path.exists():
            try:
                write_config_updates(config_path, runtime_updates)
            except Exception:
                logger.exception("Failed to write config file")
                return HTMLResponse(
                    '<div class="config-feedback warning" role="alert">'
                    "Applied to running config but failed to save to file. "
                    "Changes will be lost on restart.</div>",
                    status_code=200,
                )

        # Sync agent settings to runtime_config DB table
        if "agents" in runtime_updates:
            try:
                conn = init_db(app.state.db_path)
                try:
                    sync_agent_config_to_db(conn, cfg.agents)
                finally:
                    conn.close()
            except Exception:
                logger.exception("Failed to sync agent config to DB")

    # Write structural updates to YAML only (NOT in-memory)
    if structural_updates:
        if config_path and config_path.exists():
            try:
                write_structural_config_updates(
                    config_path, structural_updates,
                )
                app.state.restart_required = True
            except Exception:
                logger.exception("Failed to write structural config")
                return HTMLResponse(
                    '<div class="config-feedback error" role="alert">'
                    "Failed to save structural changes to file.</div>",
                    status_code=500,
                )
        else:
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Cannot save structural changes: no config file path.</div>",
                status_code=400,
            )

    msg = "Config updated successfully."
    if structural_updates:
        msg = (
            "Config saved to file. "
            "Restart required to apply structural changes."
        )
    return HTMLResponse(
        f'<div class="config-feedback success" role="alert">'
        f"{msg}</div>",
        status_code=200,
    )


# --- Identity credentials management ---

# Maps (role, field) to the env var name used in .env / config.yaml
_IDENTITY_SECRET_FIELDS: dict[tuple[str, str], str] = {
    ("coder", "github_token"): "CODER_GITHUB_TOKEN",
    ("coder", "linear_api_key"): "CODER_LINEAR_API_KEY",
    ("reviewer", "github_token"): "REVIEWER_GITHUB_TOKEN",
    ("reviewer", "linear_api_key"): "REVIEWER_LINEAR_API_KEY",
}

# Non-secret fields written directly to config.yaml
_IDENTITY_PLAIN_FIELDS: dict[str, set[str]] = {
    "coder": {"ssh_key_path", "git_author_name", "git_author_email"},
}


def _identity_status(app) -> dict:
    """Build identity status for template display."""
    cfg = app.state.botfarm_config
    if cfg is None:
        return {"configured": False}

    coder = cfg.identities.coder
    reviewer = cfg.identities.reviewer

    ssh_path = coder.ssh_key_path
    ssh_exists = False
    if ssh_path:
        try:
            ssh_exists = Path(ssh_path).expanduser().exists()
        except (OSError, ValueError):
            pass

    return {
        "configured": True,
        "coder": {
            "github_token": _mask_secret(coder.github_token),
            "github_token_set": bool(coder.github_token),
            "ssh_key_path": ssh_path,
            "ssh_key_exists": ssh_exists,
            "git_author_name": coder.git_author_name,
            "git_author_email": coder.git_author_email,
            "linear_api_key": _mask_secret(coder.linear_api_key),
            "linear_api_key_set": bool(coder.linear_api_key),
        },
        "reviewer": {
            "github_token": _mask_secret(reviewer.github_token),
            "github_token_set": bool(reviewer.github_token),
            "linear_api_key": _mask_secret(reviewer.linear_api_key),
            "linear_api_key_set": bool(reviewer.linear_api_key),
        },
    }


def _resolve_env_path(app) -> Path:
    """Resolve the .env file path from config source dir."""
    cfg = app.state.botfarm_config
    if cfg and cfg.source_path:
        return Path(cfg.source_path).parent / ".env"
    return Path.home() / ".botfarm" / ".env"


def _write_env_file(env_path: Path, data: dict[str, str]) -> None:
    """Write key-value pairs to a .env file, preserving comments."""
    lines: list[str] = []
    existing_keys: set[str] = set()

    if env_path.exists():
        for line in env_path.read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key = stripped.partition("=")[0].strip()
                if key in data:
                    lines.append(f'{key}="{data[key]}"')
                    existing_keys.add(key)
                    continue
            lines.append(line)

    # Append new keys not already in the file
    for key, value in data.items():
        if key not in existing_keys:
            lines.append(f'{key}="{value}"')

    env_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines) + "\n"
    # Atomic write: temp file + os.replace to avoid partial writes
    import tempfile
    import os
    fd, tmp_path = tempfile.mkstemp(
        dir=str(env_path.parent), suffix=".env.tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp_path, str(env_path))
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


@router.get("/identities", response_class=HTMLResponse)
def identities_page(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    return templates.TemplateResponse("identities.html", {
        "request": request,
        "identity": _identity_status(app),
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.post("/identities", response_class=HTMLResponse)
async def identities_update(request: Request):
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            "Identity editing is not available.</div>",
            status_code=400,
        )

    try:
        updates = await request.json()
    except Exception:
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            "Invalid JSON body.</div>",
            status_code=400,
        )

    if not isinstance(updates, dict):
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            "Request body must be a JSON object.</div>",
            status_code=400,
        )

    # Validate: only known roles and fields
    errors: list[str] = []
    allowed_roles = {"coder", "reviewer"}
    for role, fields in updates.items():
        if role not in allowed_roles:
            errors.append(f"Unknown role: {role!r}")
            continue
        if not isinstance(fields, dict):
            errors.append(f"'{role}' must be a mapping")
            continue
        for field, value in fields.items():
            if not isinstance(value, str):
                errors.append(f"'{role}.{field}' must be a string")
                continue
            if "\n" in value or "\r" in value:
                errors.append(f"'{role}.{field}' must not contain newlines")
                continue
            is_secret = (role, field) in _IDENTITY_SECRET_FIELDS
            is_plain = field in _IDENTITY_PLAIN_FIELDS.get(role, set())
            if not is_secret and not is_plain:
                errors.append(f"'{role}.{field}' is not an editable identity field")

    if errors:
        error_html = "".join(f"<li>{html.escape(e)}</li>" for e in errors)
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            f"<strong>Validation errors:</strong><ul>{error_html}</ul></div>",
            status_code=422,
        )

    config_path = Path(cfg.source_path) if cfg.source_path else None
    env_path = _resolve_env_path(app)

    # Separate secret vs plain-text updates
    env_updates: dict[str, str] = {}
    yaml_updates: dict[str, dict[str, str]] = {}  # role -> {field: value}
    yaml_env_refs: dict[str, dict[str, str]] = {}  # role -> {field: ${VAR}}

    for role, fields in updates.items():
        for field, value in fields.items():
            if (role, field) in _IDENTITY_SECRET_FIELDS:
                env_var = _IDENTITY_SECRET_FIELDS[(role, field)]
                env_updates[env_var] = value
                yaml_env_refs.setdefault(role, {})[field] = f"${{{env_var}}}"
            else:
                yaml_updates.setdefault(role, {})[field] = value

    # Write secrets to .env
    if env_updates:
        try:
            _write_env_file(env_path, env_updates)
        except OSError as exc:
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                f"Failed to write .env file: {html.escape(str(exc))}</div>",
                status_code=500,
            )

    # Write config.yaml: plain fields as values, secret fields as ${VAR} refs
    if (yaml_updates or yaml_env_refs) and config_path and config_path.exists():
        try:
            raw = config_path.read_text()
            data = yaml.safe_load(raw)
            if not isinstance(data, dict):
                data = {}

            if "identities" not in data or not isinstance(data.get("identities"), dict):
                data["identities"] = {}

            for role in allowed_roles:
                plain = yaml_updates.get(role, {})
                refs = yaml_env_refs.get(role, {})
                if not plain and not refs:
                    continue
                if role not in data["identities"] or not isinstance(data["identities"].get(role), dict):
                    data["identities"][role] = {}
                data["identities"][role].update(plain)
                data["identities"][role].update(refs)

            write_yaml_atomic(config_path, data)
        except Exception:
            logger.exception("Failed to write identity config to YAML")
            if env_updates:
                return HTMLResponse(
                    '<div class="config-feedback warning" role="alert">'
                    "Secrets saved to .env but failed to update config.yaml. "
                    "You may need to manually add env var references.</div>",
                    status_code=200,
                )
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Failed to save identity config.</div>",
                status_code=500,
            )

    app.state.restart_required = True
    return HTMLResponse(
        '<div class="config-feedback success" role="alert">'
        "Identity credentials saved. Restart required to apply changes.</div>",
        status_code=200,
    )
