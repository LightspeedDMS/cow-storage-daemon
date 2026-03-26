"""Tests for cow-cli commands using CliRunner.

All tests use --config-dir with tmp_path for full isolation. No mocking.
"""
import json

import pytest
from click.testing import CliRunner

from cow_cli.main import cli


@pytest.fixture
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# Version / help
# ---------------------------------------------------------------------------


def test_version(runner):
    result = runner.invoke(cli, ['--version'])
    assert result.exit_code == 0
    assert '0.1.0' in result.output


def test_help_lists_commands(runner):
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'connect' in result.output
    assert 'connections' in result.output
    assert 'activate' in result.output
    assert 'update' in result.output
    assert 'disconnect' in result.output


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------


def test_connect_creates_connection(runner, tmp_path):
    result = runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://example.com', '--token', 'tok123',
    ])
    assert result.exit_code == 0
    config_file = tmp_path / 'config.json'
    data = json.loads(config_file.read_text())
    assert 'prod' in data['connections']
    assert data['connections']['prod']['url'] == 'https://example.com'


def test_connect_first_auto_activates(runner, tmp_path):
    result = runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://example.com', '--token', 'tok123',
    ])
    assert result.exit_code == 0
    assert '(active)' in result.output
    data = json.loads((tmp_path / 'config.json').read_text())
    assert data['active'] == 'prod'


def test_connect_duplicate_alias_fails(runner, tmp_path):
    runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://example.com', '--token', 'tok1',
    ])
    result = runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://other.example.com', '--token', 'tok2',
    ])
    assert result.exit_code == 1
    assert 'already exists' in result.output


def test_connect_invalid_alias_fails(runner, tmp_path):
    result = runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'bad alias!', 'https://example.com', '--token', 'tok',
    ])
    assert result.exit_code == 1
    assert 'Invalid alias' in result.output


def test_connect_normalizes_url(runner, tmp_path):
    runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://example.com/', '--token', 'tok',
    ])
    data = json.loads((tmp_path / 'config.json').read_text())
    assert data['connections']['prod']['url'] == 'https://example.com'


def test_connect_invalid_url_fails(runner, tmp_path):
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'test', 'ftp://bad', '--token', 'tok'])
    assert result.exit_code != 0
    assert 'http://' in result.output or 'https://' in result.output


# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------


def test_connections_shows_table(runner, tmp_path):
    runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://prod.example.com', '--token', 'tok1',
    ])
    runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'dev', 'https://dev.example.com', '--token', 'tok2',
    ])
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'connections'])
    assert result.exit_code == 0
    assert 'prod' in result.output
    assert 'dev' in result.output
    assert '*' in result.output  # active indicator


def test_connections_empty(runner, tmp_path):
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'connections'])
    assert result.exit_code == 0
    assert 'No connections registered' in result.output


def test_connections_json(runner, tmp_path):
    runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        'connect', 'prod', 'https://example.com', '--token', 'secret',
    ])
    result = runner.invoke(cli, [
        '--config-dir', str(tmp_path),
        '--json', 'connections',
    ])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]['alias'] == 'prod'
    # Token should NOT be in JSON output
    assert 'token' not in data[0]


# ---------------------------------------------------------------------------
# activate
# ---------------------------------------------------------------------------


def test_activate_by_alias(runner, tmp_path):
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'first', 'https://first.example.com', '--token', 'tok1'])
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'second', 'https://second.example.com', '--token', 'tok2'])
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'activate', 'second'])
    assert result.exit_code == 0
    assert 'second' in result.output
    data = json.loads((tmp_path / 'config.json').read_text())
    assert data['active'] == 'second'


def test_activate_by_url(runner, tmp_path):
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'first', 'https://first.example.com', '--token', 'tok1'])
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'second', 'https://second.example.com', '--token', 'tok2'])
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'activate', 'https://second.example.com'])
    assert result.exit_code == 0
    data = json.loads((tmp_path / 'config.json').read_text())
    assert data['active'] == 'second'


def test_activate_nonexistent_fails(runner, tmp_path):
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'activate', 'ghost'])
    assert result.exit_code == 1
    assert 'not found' in result.output


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


def test_update_token(runner, tmp_path):
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'prod', 'https://example.com', '--token', 'old'])
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'update', 'prod', '--token', 'new_tok'])
    assert result.exit_code == 0
    assert 'Updated token' in result.output
    data = json.loads((tmp_path / 'config.json').read_text())
    assert data['connections']['prod']['token'] == 'new_tok'


def test_update_nonexistent_fails(runner, tmp_path):
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'update', 'ghost', '--token', 'tok'])
    assert result.exit_code == 1
    assert 'not found' in result.output


# ---------------------------------------------------------------------------
# disconnect
# ---------------------------------------------------------------------------


def test_disconnect_removes(runner, tmp_path):
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'prod', 'https://example.com', '--token', 'tok'])
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'disconnect', 'prod'])
    assert result.exit_code == 0
    assert 'Disconnected' in result.output
    data = json.loads((tmp_path / 'config.json').read_text())
    assert 'prod' not in data['connections']


def test_disconnect_active_warns(runner, tmp_path):
    runner.invoke(cli, ['--config-dir', str(tmp_path), 'connect', 'prod', 'https://example.com', '--token', 'tok'])
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'disconnect', 'prod'])
    assert result.exit_code == 0
    # Warning goes to stderr; CliRunner mixes by default, check combined output
    assert 'Warning' in result.output


def test_disconnect_nonexistent_fails(runner, tmp_path):
    result = runner.invoke(cli, ['--config-dir', str(tmp_path), 'disconnect', 'ghost'])
    assert result.exit_code == 1
    assert 'not found' in result.output
