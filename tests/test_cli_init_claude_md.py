"""Tests for botfarm init-claude-md command."""

import pytest
from click.testing import CliRunner

from botfarm.cli import (
    _detect_project,
    _scan_ci_config,
    _scan_directory_structure,
    _scan_readme,
    main,
)


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


class TestScanReadme:
    def test_extracts_description(self, tmp_path):
        (tmp_path / "README.md").write_text("# My Project\n\nA great tool for doing things.\n")
        assert _scan_readme(tmp_path) == "A great tool for doing things."

    def test_skips_headings_and_badges(self, tmp_path):
        (tmp_path / "README.md").write_text(
            "# Title\n![badge](url)\n<div>html</div>\nActual description.\n"
        )
        assert _scan_readme(tmp_path) == "Actual description."

    def test_skips_linked_badges(self, tmp_path):
        (tmp_path / "README.md").write_text(
            "# Title\n[![build](https://img.shields.io/badge)](https://ci)\nActual description.\n"
        )
        assert _scan_readme(tmp_path) == "Actual description."

    def test_rst_skips_title(self, tmp_path):
        (tmp_path / "README.rst").write_text(
            "My Project\n==========\n\nA Python library for things.\n"
        )
        assert _scan_readme(tmp_path) == "A Python library for things."

    def test_rst_skips_overline_title(self, tmp_path):
        (tmp_path / "README.rst").write_text(
            "==========\nMy Project\n==========\n\nA Python library for things.\n"
        )
        assert _scan_readme(tmp_path) == "A Python library for things."

    def test_fallback_when_no_readme(self, tmp_path):
        assert _scan_readme(tmp_path) == "Describe your project here."

    def test_truncates_long_description(self, tmp_path):
        long_line = "A" * 400
        (tmp_path / "README.md").write_text(f"# Title\n\n{long_line}\n")
        result = _scan_readme(tmp_path)
        assert len(result) == 300
        assert result.endswith("...")

    def test_case_insensitive_readme(self, tmp_path):
        (tmp_path / "readme.md").write_text("# title\n\nLowercase readme.\n")
        assert _scan_readme(tmp_path) == "Lowercase readme."

    def test_skips_separator_lines(self, tmp_path):
        (tmp_path / "README.md").write_text("---\n===\nReal description.\n")
        assert _scan_readme(tmp_path) == "Real description."


class TestScanDirectoryStructure:
    def test_lists_directories(self, tmp_path):
        (tmp_path / "src").mkdir()
        (tmp_path / "tests").mkdir()
        result = _scan_directory_structure(tmp_path)
        assert "`src/`" in result
        assert "`tests/`" in result

    def test_skips_hidden_dirs(self, tmp_path):
        (tmp_path / ".git").mkdir()
        (tmp_path / ".vscode").mkdir()
        (tmp_path / "src").mkdir()
        result = _scan_directory_structure(tmp_path)
        assert ".git" not in result
        assert ".vscode" not in result
        assert "`src/`" in result

    def test_includes_github_dir(self, tmp_path):
        (tmp_path / ".github").mkdir()
        result = _scan_directory_structure(tmp_path)
        assert "`.github/`" in result

    def test_skips_node_modules(self, tmp_path):
        (tmp_path / "node_modules").mkdir()
        (tmp_path / "src").mkdir()
        result = _scan_directory_structure(tmp_path)
        assert "node_modules" not in result

    def test_includes_notable_files(self, tmp_path):
        (tmp_path / "Makefile").touch()
        (tmp_path / "Dockerfile").touch()
        result = _scan_directory_structure(tmp_path)
        assert "`Makefile`" in result
        assert "`Dockerfile`" in result

    def test_fallback_empty_dir(self, tmp_path):
        result = _scan_directory_structure(tmp_path)
        assert result == "Describe key modules and patterns."

    def test_caps_at_20_entries(self, tmp_path):
        for i in range(25):
            (tmp_path / f"dir{i:02d}").mkdir()
        result = _scan_directory_structure(tmp_path)
        assert "... and 5 more" in result


class TestScanCiConfig:
    def test_detects_github_actions(self, tmp_path):
        (tmp_path / ".github" / "workflows").mkdir(parents=True)
        assert "GitHub Actions" in _scan_ci_config(tmp_path)

    def test_detects_makefile(self, tmp_path):
        (tmp_path / "Makefile").touch()
        assert "Makefile" in _scan_ci_config(tmp_path)

    def test_detects_dockerfile(self, tmp_path):
        (tmp_path / "Dockerfile").touch()
        assert "Docker" in _scan_ci_config(tmp_path)

    def test_detects_multiple(self, tmp_path):
        (tmp_path / ".github" / "workflows").mkdir(parents=True)
        (tmp_path / "Makefile").touch()
        result = _scan_ci_config(tmp_path)
        assert "GitHub Actions" in result
        assert "Makefile" in result

    def test_empty_when_nothing_detected(self, tmp_path):
        assert _scan_ci_config(tmp_path) == []


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

    def test_includes_readme_description(self, runner, tmp_path):
        (tmp_path / "requirements.txt").touch()
        (tmp_path / "README.md").write_text("# My App\n\nA CLI tool for automation.\n")
        runner.invoke(main, ["init-claude-md", str(tmp_path)])
        content = (tmp_path / "CLAUDE.md").read_text()
        assert "A CLI tool for automation." in content

    def test_includes_directory_listing(self, runner, tmp_path):
        (tmp_path / "requirements.txt").touch()
        (tmp_path / "src").mkdir()
        (tmp_path / "tests").mkdir()
        runner.invoke(main, ["init-claude-md", str(tmp_path)])
        content = (tmp_path / "CLAUDE.md").read_text()
        assert "`src/`" in content
        assert "`tests/`" in content

    def test_includes_ci_section(self, runner, tmp_path):
        (tmp_path / "requirements.txt").touch()
        (tmp_path / ".github" / "workflows").mkdir(parents=True)
        runner.invoke(main, ["init-claude-md", str(tmp_path)])
        content = (tmp_path / "CLAUDE.md").read_text()
        assert "## CI / Build" in content
        assert "GitHub Actions" in content

    def test_no_ci_section_when_none_detected(self, runner, tmp_path):
        (tmp_path / "requirements.txt").touch()
        runner.invoke(main, ["init-claude-md", str(tmp_path)])
        content = (tmp_path / "CLAUDE.md").read_text()
        assert "## CI / Build" not in content
