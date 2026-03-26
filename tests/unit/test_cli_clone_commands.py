"""Tests for clone lifecycle CLI commands."""
import json
from unittest.mock import patch

import httpx
import pytest
from click.testing import CliRunner

from cow_cli.client import CowClient
from cow_cli.main import cli


def _mock_client(handler):
    """Create a CowClient that uses a MockTransport instead of real HTTP."""
    transport = httpx.MockTransport(handler)
    client = CowClient.__new__(CowClient)
    client._client = httpx.Client(
        base_url="http://localhost:8081",
        headers={"Authorization": "Bearer test-token"},
        transport=transport,
        verify=False,
    )
    return client


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def config_with_connection(tmp_path):
    """Create a config directory with a registered connection."""
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "active": "local",
        "connections": {
            "local": {"url": "http://localhost:8081", "token": "test-token"}
        }
    }))
    import os
    os.chmod(str(config_file), 0o600)
    return tmp_path


class TestCloneCommandNoConnection:
    def test_clone_without_connection_fails(self, runner, tmp_path):
        result = runner.invoke(cli, [
            '--config-dir', str(tmp_path),
            'clone', '/src', '--namespace', 'ns', '--name', 'n',
        ])
        assert result.exit_code != 0
        assert 'No active connection' in result.output

    def test_list_without_connection_fails(self, runner, tmp_path):
        result = runner.invoke(cli, [
            '--config-dir', str(tmp_path),
            'list',
        ])
        assert result.exit_code != 0
        assert 'No active connection' in result.output

    def test_info_without_connection_fails(self, runner, tmp_path):
        result = runner.invoke(cli, [
            '--config-dir', str(tmp_path),
            'info', 'ns', 'name',
        ])
        assert result.exit_code != 0
        assert 'No active connection' in result.output

    def test_delete_without_connection_fails(self, runner, tmp_path):
        result = runner.invoke(cli, [
            '--config-dir', str(tmp_path),
            'delete', 'ns', 'name', '--force',
        ])
        assert result.exit_code != 0
        assert 'No active connection' in result.output

    def test_job_without_connection_fails(self, runner, tmp_path):
        result = runner.invoke(cli, [
            '--config-dir', str(tmp_path),
            'job', 'abc-123',
        ])
        assert result.exit_code != 0
        assert 'No active connection' in result.output


class TestHelpOutput:
    def test_clone_help(self, runner):
        result = runner.invoke(cli, ['clone', '--help'])
        assert result.exit_code == 0
        assert 'source_path' in result.output.lower() or 'SOURCE_PATH' in result.output

    def test_list_help(self, runner):
        result = runner.invoke(cli, ['list', '--help'])
        assert result.exit_code == 0
        assert '--namespace' in result.output

    def test_info_help(self, runner):
        result = runner.invoke(cli, ['info', '--help'])
        assert result.exit_code == 0

    def test_delete_help(self, runner):
        result = runner.invoke(cli, ['delete', '--help'])
        assert result.exit_code == 0
        assert '--force' in result.output

    def test_job_help(self, runner):
        result = runner.invoke(cli, ['job', '--help'])
        assert result.exit_code == 0

    def test_all_commands_in_help(self, runner):
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        for cmd in ['clone', 'list', 'info', 'delete', 'job',
                     'connect', 'connections', 'activate', 'update', 'disconnect']:
            assert cmd in result.output


class TestHumanSize:
    def test_human_size_import(self):
        from cow_cli.commands.clone import _human_size
        assert _human_size(0) == "0.0 B"
        assert _human_size(1024) == "1.0 KB"
        assert _human_size(1024 * 1024) == "1.0 MB"
        assert _human_size(1024 * 1024 * 1024) == "1.0 GB"
        assert _human_size(500) == "500.0 B"


class TestCloneNowaitOutput:
    def test_nowait_prints_job_id(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(202, json={"job_id": "abc-123", "status": "pending"})

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                'clone', '/src', '--namespace', 'ns', '--name', 'c1', '--nowait',
            ])
        assert result.exit_code == 0
        assert 'abc-123' in result.output

    def test_nowait_json_output(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(202, json={"job_id": "abc-123", "status": "pending"})

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                '--json',
                'clone', '/src', '--namespace', 'ns', '--name', 'c1', '--nowait',
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["job_id"] == "abc-123"


class TestListCommandOutput:
    def test_list_shows_table_headers(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json=[
                {"namespace": "test", "name": "c1", "source_path": "/src",
                 "clone_path": "test/c1", "created_at": "2026-01-01T00:00:00Z", "size_bytes": 1048576},
            ])

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                'list',
            ])
        assert result.exit_code == 0
        assert 'NAMESPACE' in result.output
        assert 'NAME' in result.output
        assert 'test' in result.output
        assert 'c1' in result.output

    def test_list_empty_shows_message(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json=[])

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                'list',
            ])
        assert result.exit_code == 0
        assert 'No clones found' in result.output

    def test_list_json_output(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json=[
                {"namespace": "test", "name": "c1", "source_path": "/src",
                 "clone_path": "test/c1", "created_at": "2026-01-01T00:00:00Z", "size_bytes": 1024},
            ])

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                '--json', 'list',
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["name"] == "c1"


class TestInfoCommandOutput:
    def test_info_shows_fields(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={
                "namespace": "test", "name": "c1", "source_path": "/src",
                "clone_path": "test/c1", "created_at": "2026-01-01T00:00:00Z", "size_bytes": 2048,
            })

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                'info', 'test', 'c1',
            ])
        assert result.exit_code == 0
        assert 'Namespace:' in result.output
        assert 'test' in result.output
        assert 'Clone Path:' in result.output


class TestDeleteCommandOutput:
    def test_delete_force_no_prompt(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={"status": "deleted", "namespace": "test", "name": "c1"})

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                'delete', 'test', 'c1', '--force',
            ])
        assert result.exit_code == 0
        assert "Deleted clone 'test/c1'" in result.output

    def test_delete_confirmation_cancel(self, runner, config_with_connection):
        """When user says 'n' to confirmation, no delete happens."""
        result = runner.invoke(cli, [
            '--config-dir', str(config_with_connection),
            'delete', 'test', 'c1',
        ], input='n\n')
        assert 'Cancelled' in result.output or result.exit_code == 0


class TestJobCommandOutput:
    def test_job_shows_status(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={
                "job_id": "abc-123", "status": "completed",
                "namespace": "test", "name": "c1",
                "source_path": "/src", "clone_path": "test/c1",
                "completed_at": "2026-01-01T00:01:00Z",
            })

        with patch('cow_cli.commands.clone._get_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, [
                '--config-dir', str(config_with_connection),
                'job', 'abc-123',
            ])
        assert result.exit_code == 0
        assert 'completed' in result.output
        assert 'abc-123' in result.output
