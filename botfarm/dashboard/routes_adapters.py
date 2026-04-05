"""Adapter management routes — list, configure, and check agent adapters."""

from __future__ import annotations

import html
import logging
from pathlib import Path

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from botfarm.agent import build_adapter_registry, discover_adapter_schemas
from botfarm.config import AdapterConfig, write_yaml_atomic

from .state import manual_pause_state, read_state, supervisor_status

logger = logging.getLogger(__name__)

router = APIRouter()


def _adapter_data(app) -> list[dict]:
    """Build per-adapter display data from config, registry, and schemas."""
    cfg = app.state.botfarm_config
    if cfg is None:
        return []

    schemas = discover_adapter_schemas()

    # Build registry to query capabilities (uses no-config instantiation)
    try:
        registry = build_adapter_registry(auth_mode=cfg.auth_mode)
    except Exception:
        logger.debug("Failed to build adapter registry for dashboard", exc_info=True)
        registry = {}

    # Collect preflight results filtered to adapters
    preflight_by_adapter: dict[str, list[dict]] = {}
    get_results = app.state.get_preflight_results
    if get_results is not None:
        try:
            for check in get_results():
                if check.name.startswith("adapter:"):
                    parts = check.name.split(":", 2)
                    if len(parts) == 3:
                        adapter_name = parts[1]
                        preflight_by_adapter.setdefault(adapter_name, []).append({
                            "check": parts[2],
                            "passed": check.passed,
                            "message": check.message,
                            "critical": check.critical,
                        })
        except Exception:
            logger.debug("Failed to get preflight results", exc_info=True)

    # Merge all known adapters (from config + discovered schemas)
    all_names = sorted(set(cfg.agents.adapters) | set(schemas))
    adapters = []
    for name in all_names:
        adapter_cfg = cfg.agents.adapters.get(name, AdapterConfig())
        schema = schemas.get(name)
        adapter_obj = registry.get(name)

        capabilities = {}
        if adapter_obj is not None:
            capabilities = {
                "context_fill": adapter_obj.supports_context_fill,
                "max_turns": adapter_obj.supports_max_turns,
                "model_override": adapter_obj.supports_model_override,
            }

        adapters.append({
            "name": name,
            "enabled": adapter_cfg.enabled,
            "model": adapter_cfg.model,
            "timeout_minutes": adapter_cfg.timeout_minutes,
            "reasoning_effort": adapter_cfg.reasoning_effort,
            "skip_on_reiteration": adapter_cfg.skip_on_reiteration,
            "description": schema.description if schema else "",
            "schema_fields": [
                {
                    "name": f.name,
                    "type": f.field_type.__name__,
                    "default": f.default,
                    "description": f.description,
                }
                for f in (schema.fields if schema else [])
            ],
            "required_env_vars": schema.required_env_vars if schema else [],
            "capabilities": capabilities,
            "preflight": preflight_by_adapter.get(name, []),
            "in_registry": name in registry,
        })

    return adapters


@router.get("/config/adapters", response_class=HTMLResponse)
def adapters_page(request: Request):
    app = request.app
    templates = app.state.templates
    cfg = app.state.botfarm_config
    state = read_state(app)
    return templates.TemplateResponse(request, "adapters.html", {
        "config_enabled": cfg is not None,
        "adapters": _adapter_data(app),
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.post("/config/adapters", response_class=HTMLResponse)
async def adapters_update(request: Request):
    """Save adapter configuration changes.

    Expects JSON: ``{"<adapter_name>": {"enabled": bool, "model": str, ...}, ...}``
    """
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

    # Validate
    errors: list[str] = []
    for adapter_name, fields in updates.items():
        if not isinstance(fields, dict):
            errors.append(f"{adapter_name}: expected an object")
            continue
        if "timeout_minutes" in fields:
            val = fields["timeout_minutes"]
            if val is not None and val != "":
                try:
                    val = int(val)
                    if val < 1:
                        errors.append(f"{adapter_name}: timeout_minutes must be at least 1")
                except (TypeError, ValueError):
                    errors.append(f"{adapter_name}: timeout_minutes must be an integer")
        if "reasoning_effort" in fields:
            val = fields["reasoning_effort"]
            if val and val not in ("none", "low", "medium", "high", "xhigh"):
                errors.append(
                    f"{adapter_name}: reasoning_effort must be one of: "
                    "none, low, medium, high, xhigh"
                )

    if errors:
        error_html = "".join(f"<li>{html.escape(e)}</li>" for e in errors)
        return HTMLResponse(
            '<div class="config-feedback error" role="alert">'
            f"<strong>Validation errors:</strong><ul>{error_html}</ul></div>",
            status_code=422,
        )

    # Apply to in-memory config
    for adapter_name, fields in updates.items():
        adapter_cfg = cfg.agents.adapters.get(adapter_name)
        if adapter_cfg is None:
            adapter_cfg = AdapterConfig()
            cfg.agents.adapters[adapter_name] = adapter_cfg

        if "enabled" in fields:
            adapter_cfg.enabled = bool(fields["enabled"])
        if "model" in fields:
            adapter_cfg.model = str(fields["model"])
        if "timeout_minutes" in fields:
            val = fields["timeout_minutes"]
            adapter_cfg.timeout_minutes = int(val) if val not in (None, "") else None
        if "reasoning_effort" in fields:
            adapter_cfg.reasoning_effort = str(fields["reasoning_effort"] or "")
        if "skip_on_reiteration" in fields:
            adapter_cfg.skip_on_reiteration = bool(fields["skip_on_reiteration"])

    # Persist to YAML
    config_path = Path(cfg.source_path) if cfg.source_path else None
    if config_path and config_path.exists():
        try:
            raw = config_path.read_text()
            data = yaml.safe_load(raw)
            if isinstance(data, dict):
                agents_block = data.setdefault("agents", {})
                adapters_block = agents_block.setdefault("adapters", {})
                for adapter_name in updates:
                    acfg = cfg.agents.adapters[adapter_name]
                    adapter_yaml = adapters_block.setdefault(adapter_name, {})
                    adapter_yaml["enabled"] = acfg.enabled
                    adapter_yaml["model"] = acfg.model
                    adapter_yaml["timeout_minutes"] = acfg.timeout_minutes
                    adapter_yaml["reasoning_effort"] = acfg.reasoning_effort
                    adapter_yaml["skip_on_reiteration"] = acfg.skip_on_reiteration
                write_yaml_atomic(config_path, data)
        except Exception:
            logger.warning("Failed to persist adapter config to YAML", exc_info=True)
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Changes applied in-memory but failed to persist to config file.</div>",
                status_code=500,
            )

    return HTMLResponse(
        '<div class="config-feedback success" role="alert">'
        "Adapter configuration saved.</div>"
    )


@router.post("/api/adapter-preflight", response_class=JSONResponse)
def adapter_preflight(request: Request):
    """Trigger a preflight recheck specifically for adapters.

    Re-uses the supervisor's full preflight rerun callback if available,
    then returns the adapter-specific results.
    """
    app = request.app
    cb = app.state.on_rerun_preflight
    if cb is not None:
        cb()
    return JSONResponse({"status": "ok"})
