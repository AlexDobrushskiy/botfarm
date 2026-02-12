# Botfarm

Autonomous Linear ticket dispatcher for Claude Code agents.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Usage

```bash
botfarm --help
botfarm status
botfarm history
botfarm limits
```

## Testing

```bash
python -m pytest tests/ -v
```
