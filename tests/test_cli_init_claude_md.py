"""Tests for botfarm init-claude-md command."""

import pytest
from click.testing import CliRunner

from botfarm.cli import _detect_project, main


@pytest.fixture()
def runner():
    return CliRunner()


class TestDetectProject:
    def test_python_requirements_txt(self, tmp_path):
        (tmp_path / "requirements.txt").touch()
        result = _detect_project(tmp_path)
        assert result["language"] == "Python"
        assert "pytest" in result["test_command"]
        assert result["marker"] == "requirements.txt"

    def test_python_pyproject_toml(self, tmp_path):
        (tmp_path / "pyproject.toml").touch()
        result = _detect_project(tmp_path)
        assert result["language"] == "Python"
        assert result["marker"] == "pyproject.toml"

    def test_node_package_json(self, tmp_path):
        (tmp_path / "package.json").touch()
        result = _detect_project(tmp_path)
        assert result["language"] == "Node.js"
        assert "npm" in result["test_command"]

    def test_go_mod(self, tmp_path):
        (tmp_path / "go.mod").touch()
        result = _detect_project(tmp_path)
        assert result["language"] == "Go"
        assert "go test" in result["test_command"]

    def test_cargo_toml(self, tmp_path):
        (tmp_path / "Cargo.toml").touch()
        result = _detect_project(tmp_path)
        assert result["language"] == "Rust"
        assert "cargo test" in result["test_command"]

    def test_gemfile(self, tmp_path):
        (tmp_path / "Gemfile").touch()
        result = _detect_project(tmp_path)
        assert result["language"] == "Ruby"
        assert "rspec" in result["test_command"]

    def test_unknown_project(self, tmp_path):
        result = _detect_project(tmp_path)
        assert result["language"] == "Unknown"
        assert result["marker"] is None

    def test_first_marker_wins(self, tmp_path):
        # requirements.txt comes before pyproject.toml in the list
        (tmp_path / "requirements.txt").touch()
        (tmp_path / "pyproject.toml").touch()
        result = _detect_project(tmp_path)
        assert result["marker"] == "requirements.txt"


class TestInitClaudeMd:
    def test_generates_template(self, runner, tmp_path):
        (tmp_path / "package.json").touch()
        result = runner.invoke(main, ["init-claude-md", str(tmp_path)])
        assert result.exit_code == 0
        claude_md = tmp_path / "CLAUDE.md"
        assert claude_md.exists()
        content = claude_md.read_text()
        assert "npm test" in content
        assert "npm install" in content
        assert tmp_path.name in content

    def test_refuses_overwrite(self, runner, tmp_path):
        (tmp_path / "CLAUDE.md").write_text("existing")
        result = runner.invoke(main, ["init-claude-md", str(tmp_path)])
        assert result.exit_code == 1
        assert "already exists" in result.output
        # Content should be unchanged
        assert (tmp_path / "CLAUDE.md").read_text() == "existing"

    def test_unknown_project_generic_template(self, runner, tmp_path):
        result = runner.invoke(main, ["init-claude-md", str(tmp_path)])
        assert result.exit_code == 0
        assert "No known project markers" in result.output
        content = (tmp_path / "CLAUDE.md").read_text()
        assert "Add your test command here" in content

    def test_prints_detected_language(self, runner, tmp_path):
        (tmp_path / "go.mod").touch()
        result = runner.invoke(main, ["init-claude-md", str(tmp_path)])
        assert result.exit_code == 0
        assert "Detected: Go" in result.output
        assert "go.mod" in result.output

    def test_nonexistent_dir(self, runner, tmp_path):
        result = runner.invoke(main, ["init-claude-md", str(tmp_path / "nope")])
        assert result.exit_code != 0

    def test_template_sections(self, runner, tmp_path):
        (tmp_path / "Cargo.toml").touch()
        runner.invoke(main, ["init-claude-md", str(tmp_path)])
        content = (tmp_path / "CLAUDE.md").read_text()
        assert "## Project Context" in content
        assert "## Testing" in content
        assert "## Development" in content
        assert "## Architecture" in content
        assert "## Conventions" in content
