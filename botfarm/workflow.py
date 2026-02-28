"""Load pipeline definitions from the database and render prompt templates."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


@dataclass
class StageTemplate:
    name: str
    stage_order: int
    executor_type: str  # "claude", "shell", "internal"
    identity: str | None  # "coder", "reviewer", or None
    prompt_template: str | None
    max_turns: int | None
    timeout_minutes: int | None
    shell_command: str | None
    result_parser: str | None  # "pr_url", "review_verdict", etc.


@dataclass
class StageLoop:
    name: str
    start_stage: str
    end_stage: str
    max_iterations: int
    config_key: str | None  # Config override key, e.g. "max_review_iterations"
    exit_condition: str | None
    on_failure_stage: str | None


@dataclass
class PipelineTemplate:
    id: int
    name: str  # "implementation", "investigation"
    description: str | None
    ticket_label: str | None  # Label that selects this pipeline
    is_default: bool
    stages: list[StageTemplate] = field(default_factory=list)
    loops: list[StageLoop] = field(default_factory=list)


class _SafeDict(dict):
    """Dict subclass for str.format_map() that leaves unknown keys unchanged."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def load_pipeline(conn: sqlite3.Connection, ticket_labels: list[str]) -> PipelineTemplate:
    """Load the appropriate pipeline template based on ticket labels.

    Resolution order:
    1. Find pipeline whose ticket_label matches any of the ticket's labels (case-insensitive)
    2. Fall back to the default pipeline (is_default=True)
    3. Raise error if no match found
    """
    labels_lower = {lbl.lower() for lbl in ticket_labels} if ticket_labels else set()

    if labels_lower:
        rows = conn.execute(
            "SELECT * FROM pipeline_templates WHERE ticket_label IS NOT NULL"
        ).fetchall()
        for row in rows:
            if row["ticket_label"].lower() in labels_lower:
                return _build_pipeline(conn, row)

    # Fall back to default
    row = conn.execute(
        "SELECT * FROM pipeline_templates WHERE is_default = 1"
    ).fetchone()
    if row is None:
        raise RuntimeError("No matching pipeline found and no default pipeline configured")
    return _build_pipeline(conn, row)


def load_pipeline_by_name(conn: sqlite3.Connection, name: str) -> PipelineTemplate:
    """Load a specific pipeline template by name."""
    row = conn.execute(
        "SELECT * FROM pipeline_templates WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Pipeline {name!r} not found")
    return _build_pipeline(conn, row)


def render_prompt(stage: StageTemplate, **variables: str) -> str:
    """Render a stage's prompt template with runtime variables.

    Uses str.format_map() with a SafeDict that leaves unknown placeholders
    unchanged, so optional variables don't cause errors.
    """
    if stage.prompt_template is None:
        raise ValueError(f"Stage {stage.name!r} has no prompt template")
    return stage.prompt_template.format_map(_SafeDict(variables))


def get_stage(pipeline: PipelineTemplate, stage_name: str) -> StageTemplate:
    """Get a stage by name from the pipeline."""
    for stage in pipeline.stages:
        if stage.name == stage_name:
            return stage
    raise ValueError(f"Stage {stage_name!r} not found in pipeline {pipeline.name!r}")


def get_loop_for_stage(pipeline: PipelineTemplate, stage_name: str) -> StageLoop | None:
    """Get the loop definition that contains a given stage, if any."""
    for loop in pipeline.loops:
        if loop.start_stage == stage_name or loop.end_stage == stage_name:
            return loop
    return None


def resolve_max_iterations(loop: StageLoop, agents_cfg: object) -> int:
    """Resolve effective max iterations: check config_key override first, fall back to loop default."""
    if loop.config_key is not None:
        value = getattr(agents_cfg, loop.config_key, None)
        if value is not None:
            return int(value)
    return loop.max_iterations


def _build_pipeline(conn: sqlite3.Connection, row: sqlite3.Row) -> PipelineTemplate:
    """Build a PipelineTemplate from a DB row, loading stages and loops."""
    pipeline_id = row["id"]

    stage_rows = conn.execute(
        "SELECT * FROM stage_templates WHERE pipeline_id = ? ORDER BY stage_order",
        (pipeline_id,),
    ).fetchall()

    stages = [
        StageTemplate(
            name=s["name"],
            stage_order=s["stage_order"],
            executor_type=s["executor_type"],
            identity=s["identity"],
            prompt_template=s["prompt_template"],
            max_turns=s["max_turns"],
            timeout_minutes=s["timeout_minutes"],
            shell_command=s["shell_command"],
            result_parser=s["result_parser"],
        )
        for s in stage_rows
    ]

    loop_rows = conn.execute(
        "SELECT * FROM stage_loops WHERE pipeline_id = ?",
        (pipeline_id,),
    ).fetchall()

    loops = [
        StageLoop(
            name=lp["name"],
            start_stage=lp["start_stage"],
            end_stage=lp["end_stage"],
            max_iterations=lp["max_iterations"],
            config_key=lp["config_key"],
            exit_condition=lp["exit_condition"],
            on_failure_stage=lp["on_failure_stage"],
        )
        for lp in loop_rows
    ]

    return PipelineTemplate(
        id=pipeline_id,
        name=row["name"],
        description=row["description"],
        ticket_label=row["ticket_label"],
        is_default=bool(row["is_default"]),
        stages=stages,
        loops=loops,
    )
