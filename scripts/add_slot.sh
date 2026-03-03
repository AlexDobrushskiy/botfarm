#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PARENT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

SLOT_PREFIX="botfarm-slot-"
CONFIG_FILE="$HOME/.botfarm/config.yaml"

# Prune stale worktree references (e.g. manually deleted slot directories)
git -C "$PROJECT_DIR" worktree prune

# Determine next slot number by scanning existing slot directories
next_slot=1
for dir in "$PARENT_DIR"/${SLOT_PREFIX}*; do
    [ -d "$dir" ] || continue
    basename="$(basename "$dir")"
    num="${basename#"$SLOT_PREFIX"}"
    if [[ "$num" =~ ^[0-9]+$ ]] && [ "$num" -ge "$next_slot" ]; then
        next_slot=$((num + 1))
    fi
done

SLOT_NUM="$next_slot"
SLOT_DIR="$PARENT_DIR/${SLOT_PREFIX}${SLOT_NUM}"
BRANCH_NAME="slot-${SLOT_NUM}-placeholder"

echo "=== Creating botfarm worker slot $SLOT_NUM ==="
echo "Directory: $SLOT_DIR"
echo "Branch:    $BRANCH_NAME"
echo ""

# 1. Add git worktree
echo "--- Adding git worktree ---"
git -C "$PROJECT_DIR" worktree add -b "$BRANCH_NAME" "$SLOT_DIR"
echo ""

# 2. Create virtualenv and install Python dependencies
echo "--- Setting up Python virtualenv ---"
python3 -m venv "$SLOT_DIR/.venv"
"$SLOT_DIR/.venv/bin/pip" install --upgrade pip
"$SLOT_DIR/.venv/bin/pip" install -r "$SLOT_DIR/requirements.txt"
"$SLOT_DIR/.venv/bin/pip" install -e "$SLOT_DIR"
echo ""

# 3. Configure git hooks in the worktree
if [ -d "$SLOT_DIR/.githooks" ]; then
    echo "--- Configuring git hooks ---"
    git -C "$SLOT_DIR" config core.hooksPath .githooks
    echo "Hooks path set to .githooks"
    echo ""
fi

# 4. Register slot in config.yaml
if [ -f "$CONFIG_FILE" ]; then
    echo "--- Updating config.yaml ---"
    # Check if the slot is already registered for the botfarm project
    if python3 -c "
import yaml, sys
with open('$CONFIG_FILE') as f:
    cfg = yaml.safe_load(f)
for proj in cfg.get('projects', []):
    if proj.get('name') == 'botfarm':
        if $SLOT_NUM in proj.get('slots', []):
            sys.exit(0)  # already registered
        else:
            sys.exit(1)  # needs registration
sys.exit(2)  # project not found
" 2>/dev/null; then
        echo "Slot $SLOT_NUM already registered in config.yaml"
    else
        python3 -c "
import yaml

config_path = '$CONFIG_FILE'
with open(config_path) as f:
    cfg = yaml.safe_load(f)

for proj in cfg.get('projects', []):
    if proj.get('name') == 'botfarm':
        slots = proj.get('slots', [])
        if $SLOT_NUM not in slots:
            slots.append($SLOT_NUM)
            slots.sort()
            proj['slots'] = slots
        break

with open(config_path, 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
"
        echo "Registered slot $SLOT_NUM in config.yaml"
    fi
    echo ""
else
    echo "WARNING: $CONFIG_FILE not found — add slot $SLOT_NUM to your config manually"
    echo "  Under the botfarm project, set: slots: [1, $SLOT_NUM]"
    echo ""
fi

echo "=== Slot $SLOT_NUM ready at $SLOT_DIR ==="
echo ""
echo "The supervisor will use this slot automatically on next restart."
echo "SQLite sandbox DB is created at runtime by the supervisor."
