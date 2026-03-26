"""Tests for health and stats CLI commands."""
import json
import os

import httpx
import pytest
from click.testing import CliRunner
from unittest.mock import patch

from cow_cli.client import CowClient
from cow_cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def config_with_connection(tmp_path):
    """Config with a registered active connection."""
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "active": "local",
        "connections": {
            "local": {"url": "http://localhost:8091", "token": "test-token"}
        }
    }))
    os.chmod(str(config_file), 0o600)
    return tmp_path


@pytest.fixture
def config_with_two_connections(tmp_path):
    """Config with two connections for --all testing."""
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "active": "local",
        "connections": {
            "local": {"url": "http://localhost:8091", "token": "tok1"},
            "remote": {"url": "http://remote:8091", "token": "tok2"}
        }
    }))
    os.chmod(str(config_file), 0o600)
    return tmp_path


def _mock_client(handler):
    """Create a CowClient with MockTransport."""
    transport = httpx.MockTransport(handler)
    client = CowClient.__new__(CowClient)
    client._client = httpx.Client(
        base_url="http://localhost:8091",
        headers={"Authorization": "Bearer test-token"},
        transport=transport,
        verify=False,
    )
    return client


class TestHealthNoConnection:
    def test_health_no_connection(self, runner, tmp_path):
        result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'health'])
        assert result.exit_code != 0
        assert 'No active connection' in result.output

    def test_stats_no_connection(self, runner, tmp_path):
        result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'stats'])
        assert result.exit_code != 0
        assert 'No active connection' in result.output


class TestHealthCommand:
    def test_health_human_output(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={
                "status": "healthy",
                "filesystem_type": "xfs",
                "cow_method": "reflink",
                "disk_total_bytes": 1073741824,
                "disk_used_bytes": 536870912,
                "disk_available_bytes": 536870912,
                "uptime_seconds": 3661.5,
            })

        with patch('cow_cli.commands.health._get_active_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, ['--config-dir', str(config_with_connection), 'health'])
        assert result.exit_code == 0
        assert 'healthy' in result.output
        assert 'xfs' in result.output
        assert 'reflink' in result.output
        assert '1h 1m' in result.output

    def test_health_json_output(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={
                "status": "healthy",
                "filesystem_type": "xfs",
                "cow_method": "reflink",
                "disk_total_bytes": 1073741824,
                "disk_used_bytes": 536870912,
                "disk_available_bytes": 536870912,
                "uptime_seconds": 60.0,
            })

        with patch('cow_cli.commands.health._get_active_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, ['--config-dir', str(config_with_connection), '--json', 'health'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "healthy"


class TestHealthAllCommand:
    def test_health_all_table(self, runner, config_with_two_connections):
        call_count = 0
        def handler(request):
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json={
                "status": "healthy",
                "filesystem_type": "xfs",
                "cow_method": "reflink",
                "disk_total_bytes": 1073741824,
                "disk_used_bytes": 536870912,
                "disk_available_bytes": 536870912,
                "uptime_seconds": 120.0,
            })

        with patch('cow_cli.commands.health.CowClient', side_effect=lambda **kwargs: _mock_client(handler)):
            result = runner.invoke(cli, ['--config-dir', str(config_with_two_connections), 'health', '--all'])
        assert result.exit_code == 0
        assert 'ALIAS' in result.output
        assert 'STATUS' in result.output

    def test_health_all_empty(self, runner, tmp_path):
        result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'health', '--all'])
        assert 'No connections' in result.output


class TestStatsCommand:
    def test_stats_human_output(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={
                "disk_total_bytes": 214748364800,
                "disk_used_bytes": 107374182400,
                "disk_available_bytes": 107374182400,
                "clone_count_total": 5,
                "clones_by_namespace": {"prod": 3, "staging": 2},
            })

        with patch('cow_cli.commands.health._get_active_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, ['--config-dir', str(config_with_connection), 'stats'])
        assert result.exit_code == 0
        assert 'Total Clones:' in result.output
        assert '5' in result.output
        assert 'prod' in result.output
        assert 'staging' in result.output

    def test_stats_json_output(self, runner, config_with_connection):
        def handler(request):
            return httpx.Response(200, json={
                "disk_total_bytes": 214748364800,
                "disk_used_bytes": 107374182400,
                "disk_available_bytes": 107374182400,
                "clone_count_total": 5,
                "clones_by_namespace": {"prod": 3},
            })

        with patch('cow_cli.commands.health._get_active_client', return_value=_mock_client(handler)):
            result = runner.invoke(cli, ['--config-dir', str(config_with_connection), '--json', 'stats'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["clone_count_total"] == 5


class TestHelpOutput:
    def test_health_help(self, runner):
        result = runner.invoke(cli, ['health', '--help'])
        assert result.exit_code == 0
        assert '--all' in result.output

    def test_stats_help(self, runner):
        result = runner.invoke(cli, ['stats', '--help'])
        assert result.exit_code == 0

    def test_all_12_commands_in_help(self, runner):
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        for cmd in ['health', 'stats', 'clone', 'list', 'info', 'delete', 'job',
                     'connect', 'connections', 'activate', 'update', 'disconnect']:
            assert cmd in result.output
