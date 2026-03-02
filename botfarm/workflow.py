"""Load pipeline definitions from the database, render prompt templates, and CRUD operations."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


@dataclass
class StageTemplate:
    id: int
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
    id: int
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


def load_all_pipelines(conn: sqlite3.Connection) -> list[PipelineTemplate]:
    """Load all pipeline templates from the database."""
    rows = conn.execute(
        "SELECT * FROM pipeline_templates ORDER BY is_default DESC, name"
    ).fetchall()
    return [_build_pipeline(conn, row) for row in rows]


def _build_pipeline(conn: sqlite3.Connection, row: sqlite3.Row) -> PipelineTemplate:
    """Build a PipelineTemplate from a DB row, loading stages and loops."""
    pipeline_id = row["id"]

    stage_rows = conn.execute(
        "SELECT * FROM stage_templates WHERE pipeline_id = ? ORDER BY stage_order",
        (pipeline_id,),
    ).fetchall()

    stages = [
        StageTemplate(
            id=s["id"],
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
            id=lp["id"],
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


# ---------------------------------------------------------------------------
# Pipeline CRUD
# ---------------------------------------------------------------------------


def create_pipeline(
    conn: sqlite3.Connection,
    name: str,
    description: str | None = None,
    ticket_label: str | None = None,
    is_default: bool = False,
) -> int:
    """Insert a new pipeline template and return its ID.

    If *is_default* is True, any existing default pipeline is unset first.
    """
    if is_default:
        conn.execute("UPDATE pipeline_templates SET is_default = 0 WHERE is_default = 1")
    cur = conn.execute(
        "INSERT INTO pipeline_templates (name, description, ticket_label, is_default) "
        "VALUES (?, ?, ?, ?)",
        (name, description, ticket_label, int(is_default)),
    )
    conn.commit()
    return cur.lastrowid


def update_pipeline(conn: sqlite3.Connection, pipeline_id: int, **kwargs: object) -> None:
    """Update pipeline fields. Accepted keys: name, description, ticket_label, is_default."""
    allowed = {"name", "description", "ticket_label", "is_default"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise ValueError(f"Unknown fields: {unknown}")
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    if "is_default" in updates and updates["is_default"]:
        conn.execute("UPDATE pipeline_templates SET is_default = 0 WHERE is_default = 1")
        updates["is_default"] = 1
    elif "is_default" in updates:
        updates["is_default"] = int(updates["is_default"])
    set_clause = ", ".join(f"{col} = ?" for col in updates)
    conn.execute(
        f"UPDATE pipeline_templates SET {set_clause} WHERE id = ?",
        (*updates.values(), pipeline_id),
    )
    conn.commit()


def delete_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> None:
    """Delete a pipeline and cascade to its stages and loops.

    Raises ValueError if the pipeline is the only default pipeline.
    """
    row = conn.execute(
        "SELECT is_default FROM pipeline_templates WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Pipeline {pipeline_id} not found")
    if row["is_default"]:
        other_default = conn.execute(
            "SELECT COUNT(*) AS cnt FROM pipeline_templates WHERE is_default = 1 AND id != ?",
            (pipeline_id,),
        ).fetchone()["cnt"]
        if other_default == 0:
            raise ValueError("Cannot delete the only default pipeline")
    conn.execute("DELETE FROM stage_loops WHERE pipeline_id = ?", (pipeline_id,))
    conn.execute("DELETE FROM stage_templates WHERE pipeline_id = ?", (pipeline_id,))
    conn.execute("DELETE FROM pipeline_templates WHERE id = ?", (pipeline_id,))
    conn.commit()


def duplicate_pipeline(
    conn: sqlite3.Connection, pipeline_id: int, new_name: str
) -> int:
    """Deep-copy a pipeline (with all stages and loops) under *new_name*."""
    row = conn.execute(
        "SELECT * FROM pipeline_templates WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Pipeline {pipeline_id} not found")
    cur = conn.execute(
        "INSERT INTO pipeline_templates (name, description, ticket_label, is_default) "
        "VALUES (?, ?, ?, 0)",
        (new_name, row["description"], row["ticket_label"]),
    )
    new_id = cur.lastrowid

    stage_rows = conn.execute(
        "SELECT * FROM stage_templates WHERE pipeline_id = ? ORDER BY stage_order",
        (pipeline_id,),
    ).fetchall()
    for s in stage_rows:
        conn.execute(
            "INSERT INTO stage_templates "
            "(pipeline_id, name, stage_order, executor_type, identity, "
            "prompt_template, max_turns, timeout_minutes, shell_command, result_parser) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                new_id, s["name"], s["stage_order"], s["executor_type"], s["identity"],
                s["prompt_template"], s["max_turns"], s["timeout_minutes"],
                s["shell_command"], s["result_parser"],
            ),
        )

    loop_rows = conn.execute(
        "SELECT * FROM stage_loops WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchall()
    for lp in loop_rows:
        conn.execute(
            "INSERT INTO stage_loops "
            "(pipeline_id, name, start_stage, end_stage, max_iterations, "
            "config_key, exit_condition, on_failure_stage) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                new_id, lp["name"], lp["start_stage"], lp["end_stage"],
                lp["max_iterations"], lp["config_key"], lp["exit_condition"],
                lp["on_failure_stage"],
            ),
        )

    conn.commit()
    return new_id


# ---------------------------------------------------------------------------
# Stage CRUD
# ---------------------------------------------------------------------------


def create_stage(
    conn: sqlite3.Connection,
    pipeline_id: int,
    name: str,
    stage_order: int,
    executor_type: str,
    identity: str | None = None,
    prompt_template: str | None = None,
    max_turns: int | None = None,
    timeout_minutes: int | None = None,
    shell_command: str | None = None,
    result_parser: str | None = None,
) -> int:
    """Insert a new stage. Shifts existing stage_order values up if inserting in the middle."""
    conn.execute(
        "UPDATE stage_templates SET stage_order = stage_order + 1 "
        "WHERE pipeline_id = ? AND stage_order >= ?",
        (pipeline_id, stage_order),
    )
    cur = conn.execute(
        "INSERT INTO stage_templates "
        "(pipeline_id, name, stage_order, executor_type, identity, "
        "prompt_template, max_turns, timeout_minutes, shell_command, result_parser) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            pipeline_id, name, stage_order, executor_type, identity,
            prompt_template, max_turns, timeout_minutes, shell_command, result_parser,
        ),
    )
    conn.commit()
    return cur.lastrowid


def update_stage(conn: sqlite3.Connection, stage_id: int, **kwargs: object) -> None:
    """Update any stage field."""
    allowed = {
        "name", "executor_type", "identity",
        "prompt_template", "max_turns", "timeout_minutes",
        "shell_command", "result_parser",
    }
    unknown = set(kwargs) - allowed
    if unknown:
        raise ValueError(f"Unknown fields: {unknown}")
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{col} = ?" for col in updates)
    conn.execute(
        f"UPDATE stage_templates SET {set_clause} WHERE id = ?",
        (*updates.values(), stage_id),
    )
    conn.commit()


def delete_stage(conn: sqlite3.Connection, stage_id: int) -> None:
    """Delete a stage, recompact stage_order, and clean up referencing loops."""
    row = conn.execute(
        "SELECT pipeline_id, name, stage_order FROM stage_templates WHERE id = ?",
        (stage_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Stage {stage_id} not found")
    pipeline_id = row["pipeline_id"]
    stage_name = row["name"]
    deleted_order = row["stage_order"]

    # Remove loops referencing this stage as start or end
    conn.execute(
        "DELETE FROM stage_loops WHERE pipeline_id = ? AND (start_stage = ? OR end_stage = ?)",
        (pipeline_id, stage_name, stage_name),
    )
    # Clear on_failure_stage references in remaining loops
    conn.execute(
        "UPDATE stage_loops SET on_failure_stage = NULL "
        "WHERE pipeline_id = ? AND on_failure_stage = ?",
        (pipeline_id, stage_name),
    )

    conn.execute("DELETE FROM stage_templates WHERE id = ?", (stage_id,))

    # Recompact: shift down stages that were after the deleted one
    conn.execute(
        "UPDATE stage_templates SET stage_order = stage_order - 1 "
        "WHERE pipeline_id = ? AND stage_order > ?",
        (pipeline_id, deleted_order),
    )
    conn.commit()


def reorder_stages(
    conn: sqlite3.Connection, pipeline_id: int, ordered_stage_ids: list[int]
) -> None:
    """Bulk-update stage_order to match the given ID sequence.

    Raises ValueError if *ordered_stage_ids* does not exactly match the set of
    stage IDs belonging to the pipeline.
    """
    existing = {
        r["id"]
        for r in conn.execute(
            "SELECT id FROM stage_templates WHERE pipeline_id = ?", (pipeline_id,)
        ).fetchall()
    }
    if set(ordered_stage_ids) != existing:
        raise ValueError(
            "ordered_stage_ids must contain exactly the stage IDs for the pipeline"
        )
    # Use a temporary large offset to avoid UNIQUE constraint violations during reorder
    offset = 10000
    for new_order, stage_id in enumerate(ordered_stage_ids, start=1):
        conn.execute(
            "UPDATE stage_templates SET stage_order = ? WHERE id = ? AND pipeline_id = ?",
            (new_order + offset, stage_id, pipeline_id),
        )
    for new_order, stage_id in enumerate(ordered_stage_ids, start=1):
        conn.execute(
            "UPDATE stage_templates SET stage_order = ? WHERE id = ? AND pipeline_id = ?",
            (new_order, stage_id, pipeline_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Loop CRUD
# ---------------------------------------------------------------------------


def create_loop(
    conn: sqlite3.Connection,
    pipeline_id: int,
    name: str,
    start_stage: str,
    end_stage: str,
    max_iterations: int,
    config_key: str | None = None,
    exit_condition: str | None = None,
    on_failure_stage: str | None = None,
) -> int:
    """Insert a new loop and return its ID."""
    cur = conn.execute(
        "INSERT INTO stage_loops "
        "(pipeline_id, name, start_stage, end_stage, max_iterations, "
        "config_key, exit_condition, on_failure_stage) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            pipeline_id, name, start_stage, end_stage, max_iterations,
            config_key, exit_condition, on_failure_stage,
        ),
    )
    conn.commit()
    return cur.lastrowid


def update_loop(conn: sqlite3.Connection, loop_id: int, **kwargs: object) -> None:
    """Update any loop field."""
    allowed = {
        "name", "start_stage", "end_stage", "max_iterations",
        "config_key", "exit_condition", "on_failure_stage",
    }
    unknown = set(kwargs) - allowed
    if unknown:
        raise ValueError(f"Unknown fields: {unknown}")
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{col} = ?" for col in updates)
    conn.execute(
        f"UPDATE stage_loops SET {set_clause} WHERE id = ?",
        (*updates.values(), loop_id),
    )
    conn.commit()


def delete_loop(conn: sqlite3.Connection, loop_id: int) -> None:
    """Delete a loop."""
    row = conn.execute("SELECT id FROM stage_loops WHERE id = ?", (loop_id,)).fetchone()
    if row is None:
        raise ValueError(f"Loop {loop_id} not found")
    conn.execute("DELETE FROM stage_loops WHERE id = ?", (loop_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> list[str]:
    """Return a list of validation errors for the given pipeline.

    Checks:
    - Pipeline name non-empty and unique
    - At least one stage exists
    - Stage names unique within pipeline
    - Stage orders contiguous (no gaps)
    - Loop start/end stages reference existing stages in the pipeline
    - No orphaned loops after stage deletion
    """
    errors: list[str] = []

    row = conn.execute(
        "SELECT * FROM pipeline_templates WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if row is None:
        return [f"Pipeline {pipeline_id} not found"]

    # Name non-empty
    if not row["name"] or not row["name"].strip():
        errors.append("Pipeline name must not be empty")

    # Name unique
    dup = conn.execute(
        "SELECT COUNT(*) AS cnt FROM pipeline_templates WHERE name = ? AND id != ?",
        (row["name"], pipeline_id),
    ).fetchone()["cnt"]
    if dup > 0:
        errors.append(f"Pipeline name {row['name']!r} is not unique")

    # Stages
    stage_rows = conn.execute(
        "SELECT * FROM stage_templates WHERE pipeline_id = ? ORDER BY stage_order",
        (pipeline_id,),
    ).fetchall()

    if not stage_rows:
        errors.append("Pipeline must have at least one stage")
    else:
        # Stage names unique
        stage_names = [s["name"] for s in stage_rows]
        seen: set[str] = set()
        for sn in stage_names:
            if sn in seen:
                errors.append(f"Duplicate stage name {sn!r}")
            seen.add(sn)

        # Stage orders contiguous and 1-based (1..N with no gaps)
        orders = sorted(s["stage_order"] for s in stage_rows)
        expected = list(range(1, len(orders) + 1))
        if orders != expected:
            errors.append(
                f"Stage orders are not contiguous: {orders}"
            )

    # Build lookup for validation
    stage_name_set = {s["name"] for s in stage_rows}

    # Loops
    loop_rows = conn.execute(
        "SELECT * FROM stage_loops WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchall()

    for lp in loop_rows:
        if lp["start_stage"] not in stage_name_set:
            errors.append(
                f"Loop {lp['name']!r}: start_stage {lp['start_stage']!r} "
                f"does not reference an existing stage"
            )
        if lp["end_stage"] not in stage_name_set:
            errors.append(
                f"Loop {lp['name']!r}: end_stage {lp['end_stage']!r} "
                f"does not reference an existing stage"
            )
        if lp["on_failure_stage"] and lp["on_failure_stage"] not in stage_name_set:
            errors.append(
                f"Loop {lp['name']!r}: on_failure_stage {lp['on_failure_stage']!r} "
                f"does not reference an existing stage"
            )
    return errors
