"""Tests for interactive botfarm init command and supporting functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from botfarm.linear import (
    LINEAR_API_URL,
    LinearAPIError,
    LinearClient,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _graphql_response(data: dict) -> httpx.Response:
    return httpx.Response(
        status_code=200,
        json={"data": data},
        request=httpx.Request("POST", LINEAR_API_URL),
    )


# ---------------------------------------------------------------------------
# LinearClient.list_teams
# ---------------------------------------------------------------------------


class TestListTeams:
    def test_returns_team_list(self):
        client = LinearClient(api_key="test-key")
        teams = [
            {"id": "t1", "name": "Engineering", "key": "ENG"},
            {"id": "t2", "name": "Design", "key": "DSN"},
        ]
        resp = _graphql_response({"teams": {"nodes": teams}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.list_teams()
        assert len(result) == 2
        assert result[0]["key"] == "ENG"
        assert result[1]["name"] == "Design"

    def test_returns_empty_when_no_teams(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({"teams": {"nodes": []}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.list_teams()
        assert result == []


# ---------------------------------------------------------------------------
# LinearClient.list_team_projects
# ---------------------------------------------------------------------------


class TestListTeamProjects:
    def test_returns_project_list(self):
        client = LinearClient(api_key="test-key")
        projects = [
            {"id": "p1", "name": "Bot farm"},
            {"id": "p2", "name": "Web app"},
        ]
        resp = _graphql_response(
            {"team": {"projects": {"nodes": projects}}}
        )
        with patch.object(httpx, "post", return_value=resp):
            result = client.list_team_projects("t1")
        assert len(result) == 2
        assert result[0]["name"] == "Bot farm"

    def test_returns_empty_when_team_not_found(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({"team": None})
        with patch.object(httpx, "post", return_value=resp):
            result = client.list_team_projects("nonexistent")
        assert result == []


# ---------------------------------------------------------------------------
# LinearClient.get_organization
# ---------------------------------------------------------------------------


class TestGetOrganization:
    def test_returns_organization_info(self):
        client = LinearClient(api_key="test-key")
        org = {"urlKey": "my-workspace", "name": "My Company"}
        resp = _graphql_response({"organization": org})
        with patch.object(httpx, "post", return_value=resp):
            result = client.get_organization()
        assert result["urlKey"] == "my-workspace"
        assert result["name"] == "My Company"

    def test_raises_on_missing_organization(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({"organization": None})
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="organization"):
                client.get_organization()


# ---------------------------------------------------------------------------
# _generate_config_yaml
# ---------------------------------------------------------------------------


class TestGenerateConfigYaml:
    def test_generates_with_project(self):
        from botfarm.cli import _generate_config_yaml

        content = _generate_config_yaml(
            team_key="ENG",
            team_name="Engineering",
            workspace="my-ws",
            project_name="Bot farm",
        )
        assert "linear_team: ENG" in content
        assert "workspace: my-ws" in content
        assert "linear_project: Bot farm" in content
        assert "${LINEAR_API_KEY}" in content

    def test_generates_without_project(self):
        from botfarm.cli import _generate_config_yaml

        content = _generate_config_yaml(
            team_key="ENG",
            team_name="Engineering",
            workspace="my-ws",
        )
        assert "linear_team: ENG" in content
        assert "linear_project" not in content


# ---------------------------------------------------------------------------
# _write_env_with_key
# ---------------------------------------------------------------------------


class TestWriteEnvWithKey:
    def test_writes_api_key(self, tmp_path):
        from botfarm.cli import _write_env_with_key

        env_path = tmp_path / ".env"
        _write_env_with_key(env_path, "lin_api_abc123")
        content = env_path.read_text()
        assert "LINEAR_API_KEY=lin_api_abc123" in content

    def test_creates_parent_dirs(self, tmp_path):
        from botfarm.cli import _write_env_with_key

        env_path = tmp_path / "sub" / "dir" / ".env"
        _write_env_with_key(env_path, "key123")
        assert env_path.exists()


# ---------------------------------------------------------------------------
# Interactive init CLI flow
# ---------------------------------------------------------------------------


class TestInteractiveInit:
    """Test the interactive init command via Click's CliRunner."""

    def _make_client_mock(self):
        """Create a mock LinearClient with typical responses."""
        mock_client = MagicMock(spec=LinearClient)
        mock_client.list_teams.return_value = [
            {"id": "t1", "name": "Smart AI Coach", "key": "SMA"},
        ]
        mock_client.list_team_projects.return_value = [
            {"id": "p1", "name": "Bot farm"},
            {"id": "p2", "name": "Web app"},
        ]
        mock_client.get_organization.return_value = {
            "urlKey": "smart-ai-coach",
            "name": "Smart AI Coach",
        }
        return mock_client

    @patch("botfarm.cli.LinearClient")
    def test_interactive_single_team_single_project(self, mock_cls, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        mock_client = self._make_client_mock()
        mock_client.list_team_projects.return_value = [
            {"id": "p1", "name": "Bot farm"},
        ]
        mock_cls.return_value = mock_client

        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        # Input: API key
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="lin_api_test123\n",
        )
        assert result.exit_code == 0, result.output
        assert config_path.exists()
        assert env_path.exists()

        config_content = config_path.read_text()
        assert "linear_team: SMA" in config_content
        assert "workspace: smart-ai-coach" in config_content
        assert "linear_project: Bot farm" in config_content

        env_content = env_path.read_text()
        assert "LINEAR_API_KEY=lin_api_test123" in env_content

    @patch("botfarm.cli.LinearClient")
    def test_interactive_multiple_teams_and_projects(self, mock_cls, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        mock_client = self._make_client_mock()
        mock_client.list_teams.return_value = [
            {"id": "t1", "name": "Smart AI Coach", "key": "SMA"},
            {"id": "t2", "name": "Design", "key": "DSN"},
        ]
        mock_cls.return_value = mock_client

        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        # Input: API key, team choice (1), project choice (2 = "Web app")
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="lin_api_test123\n1\n2\n",
        )
        assert result.exit_code == 0, result.output
        config_content = config_path.read_text()
        assert "linear_team: SMA" in config_content
        assert "linear_project: Web app" in config_content

    @patch("botfarm.cli.LinearClient")
    def test_interactive_no_project_filter(self, mock_cls, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        mock_client = self._make_client_mock()
        mock_cls.return_value = mock_client

        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        # Input: API key, team choice (1 = auto), project choice (3 = no filter)
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="lin_api_test123\n3\n",
        )
        assert result.exit_code == 0, result.output
        config_content = config_path.read_text()
        assert "linear_project" not in config_content

    @patch("botfarm.cli.LinearClient")
    def test_interactive_api_key_failure(self, mock_cls, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        mock_client = MagicMock(spec=LinearClient)
        mock_client.list_teams.side_effect = LinearAPIError("Unauthorized")
        mock_cls.return_value = mock_client

        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="bad_key\n",
        )
        assert result.exit_code == 0
        assert "Failed" in result.output
        assert not config_path.exists()

    @patch("botfarm.cli.LinearClient")
    def test_interactive_empty_api_key(self, mock_cls, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="\n",
        )
        assert result.exit_code == 0
        assert "empty" in result.output
        assert not config_path.exists()

    @patch("botfarm.cli.LinearClient")
    def test_interactive_workspace_fallback_to_manual(self, mock_cls, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        mock_client = self._make_client_mock()
        mock_client.get_organization.side_effect = LinearAPIError("Failed")
        mock_client.list_team_projects.return_value = []
        mock_cls.return_value = mock_client

        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        # Input: API key, manual workspace slug
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="lin_api_test123\nmy-manual-ws\n",
        )
        assert result.exit_code == 0, result.output
        config_content = config_path.read_text()
        assert "workspace: my-manual-ws" in config_content

    def test_interactive_existing_config_decline_overwrite(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from botfarm.cli import main

        config_path = tmp_path / "config.yaml"
        config_path.write_text("existing config")
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

        runner = CliRunner()
        # Input: decline overwrite (n)
        result = runner.invoke(
            main,
            ["init", "--path", str(config_path)],
            input="n\n",
        )
        assert result.exit_code == 0
        # Config should be unchanged
        assert config_path.read_text() == "existing config"
