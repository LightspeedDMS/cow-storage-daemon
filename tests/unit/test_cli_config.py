"""Tests for cow_cli.config - ConnectionConfig data layer.

Tests use real file I/O with tmp_path fixture. No mocking.
"""
import json
import os
import stat
from pathlib import Path

import pytest

from cow_cli import __version__
from cow_cli.config import (
    AliasValidationError,
    ConnectionConfig,
    DuplicateAliasError,
    InvalidConfigError,
    InvalidURLError,
)


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------


def test_version_constant():
    assert __version__ == "0.1.0"


# ---------------------------------------------------------------------------
# ConnectionConfig initialisation
# ---------------------------------------------------------------------------


def test_init_uses_default_config_dir():
    cfg = ConnectionConfig()
    assert cfg.config_dir == Path.home() / ".cow-storage"


def test_init_accepts_custom_config_dir(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    assert cfg.config_dir == tmp_path


# ---------------------------------------------------------------------------
# Load from missing / empty / corrupt file
# ---------------------------------------------------------------------------


def test_load_missing_file_returns_empty(tmp_path):
    """Missing config file → empty state, no exception."""
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.load()
    assert cfg.active is None
    assert cfg.connections == {}


def test_load_corrupt_json_raises(tmp_path):
    """Corrupted JSON → InvalidConfigError."""
    config_file = tmp_path / "config.json"
    config_file.write_text("NOT VALID JSON {{{")
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(InvalidConfigError):
        cfg.load()


def test_load_valid_config(tmp_path):
    """Valid JSON is loaded correctly."""
    data = {
        "active": "prod",
        "connections": {
            "prod": {"url": "https://prod.example.com", "token": "tok1"},
            "dev": {"url": "https://dev.example.com", "token": "tok2"},
        },
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(data))
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.load()
    assert cfg.active == "prod"
    assert len(cfg.connections) == 2
    assert cfg.connections["prod"]["url"] == "https://prod.example.com"


# ---------------------------------------------------------------------------
# Alias validation
# ---------------------------------------------------------------------------


def test_alias_validation_rejects_spaces(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(AliasValidationError):
        cfg.add("bad alias", "https://example.com", "tok")


def test_alias_validation_rejects_special_chars(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(AliasValidationError):
        cfg.add("bad@alias", "https://example.com", "tok")


def test_alias_validation_accepts_alphanumeric_hyphens_underscores(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    # Should not raise
    cfg.add("my-alias_01", "https://example.com", "token123")


def test_alias_validation_rejects_empty(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(AliasValidationError):
        cfg.add("", "https://example.com", "tok")


# ---------------------------------------------------------------------------
# URL normalisation
# ---------------------------------------------------------------------------


def test_url_trailing_slash_stripped(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("myalias", "https://example.com/api/", "tok")
    assert cfg.connections["myalias"]["url"] == "https://example.com/api"


def test_url_multiple_trailing_slashes_stripped(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("myalias", "https://example.com///", "tok")
    assert cfg.connections["myalias"]["url"] == "https://example.com"


# ---------------------------------------------------------------------------
# Add (connect) behaviour
# ---------------------------------------------------------------------------


def test_add_first_connection_auto_activates(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("first", "https://example.com", "tok")
    assert cfg.active == "first"


def test_add_second_connection_does_not_change_active(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("first", "https://first.example.com", "tok1")
    cfg.add("second", "https://second.example.com", "tok2")
    assert cfg.active == "first"


def test_add_duplicate_alias_raises(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("myalias", "https://example.com", "tok1")
    with pytest.raises(DuplicateAliasError):
        cfg.add("myalias", "https://other.example.com", "tok2")


# ---------------------------------------------------------------------------
# Save - atomic write, permissions, directory creation
# ---------------------------------------------------------------------------


def test_save_creates_config_dir_if_missing(tmp_path):
    config_dir = tmp_path / "subdir" / ".cow-storage"
    cfg = ConnectionConfig(config_dir=config_dir)
    cfg.add("test", "https://example.com", "tok")
    # Directory should be created by save
    assert config_dir.exists()


def test_save_writes_valid_json(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "mytoken")
    config_file = tmp_path / "config.json"
    data = json.loads(config_file.read_text())
    assert data["active"] == "alias1"
    assert data["connections"]["alias1"]["url"] == "https://example.com"
    assert data["connections"]["alias1"]["token"] == "mytoken"


def test_save_sets_permissions_600(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "mytoken")
    config_file = tmp_path / "config.json"
    file_stat = os.stat(config_file)
    permissions = stat.S_IMODE(file_stat.st_mode)
    assert permissions == 0o600


def test_save_is_atomic_uses_os_replace(tmp_path, monkeypatch):
    """Verify save uses atomic write (temp file + os.replace)."""
    replaced_calls = []
    original_replace = os.replace

    def spy_replace(src, dst):
        replaced_calls.append((src, dst))
        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy_replace)
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "tok")
    # os.replace must have been called
    assert len(replaced_calls) >= 1
    # destination should be the config.json path
    dst = replaced_calls[-1][1]
    assert dst == str(tmp_path / "config.json")


def test_save_roundtrip(tmp_path):
    """Data saved by one instance loads correctly into another."""
    cfg1 = ConnectionConfig(config_dir=tmp_path)
    cfg1.add("prod", "https://prod.example.com", "tok_prod")
    cfg1.add("dev", "https://dev.example.com", "tok_dev")

    cfg2 = ConnectionConfig(config_dir=tmp_path)
    cfg2.load()
    assert cfg2.active == "prod"
    assert cfg2.connections["dev"]["token"] == "tok_dev"


# ---------------------------------------------------------------------------
# Activate
# ---------------------------------------------------------------------------


def test_activate_by_alias(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("first", "https://first.example.com", "tok1")
    cfg.add("second", "https://second.example.com", "tok2")
    cfg.activate("second")
    assert cfg.active == "second"


def test_activate_by_url(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("first", "https://first.example.com", "tok1")
    cfg.add("second", "https://second.example.com", "tok2")
    cfg.activate("https://second.example.com")
    assert cfg.active == "second"


def test_activate_nonexistent_raises(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("first", "https://first.example.com", "tok1")
    with pytest.raises(KeyError):
        cfg.activate("nonexistent")


def test_activate_persists_to_disk(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("first", "https://first.example.com", "tok1")
    cfg.add("second", "https://second.example.com", "tok2")
    cfg.activate("second")

    cfg2 = ConnectionConfig(config_dir=tmp_path)
    cfg2.load()
    assert cfg2.active == "second"


# ---------------------------------------------------------------------------
# Update token
# ---------------------------------------------------------------------------


def test_update_token(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "old_token")
    cfg.update_token("alias1", "new_token")
    assert cfg.connections["alias1"]["token"] == "new_token"


def test_update_token_nonexistent_raises(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(KeyError):
        cfg.update_token("nonexistent", "new_token")


def test_update_token_persists_to_disk(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "old_token")
    cfg.update_token("alias1", "new_token")

    cfg2 = ConnectionConfig(config_dir=tmp_path)
    cfg2.load()
    assert cfg2.connections["alias1"]["token"] == "new_token"


# ---------------------------------------------------------------------------
# Remove (disconnect)
# ---------------------------------------------------------------------------


def test_remove_connection(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "tok1")
    cfg.add("alias2", "https://other.example.com", "tok2")
    cfg.remove("alias1")
    assert "alias1" not in cfg.connections
    assert "alias2" in cfg.connections


def test_remove_nonexistent_raises(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(KeyError):
        cfg.remove("nonexistent")


def test_remove_active_sets_active_to_none(tmp_path):
    """Removing the active connection sets active to None."""
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "tok1")
    assert cfg.active == "alias1"
    cfg.remove("alias1")
    assert cfg.active is None


def test_remove_non_active_keeps_active_unchanged(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://first.example.com", "tok1")
    cfg.add("alias2", "https://second.example.com", "tok2")
    assert cfg.active == "alias1"
    cfg.remove("alias2")
    assert cfg.active == "alias1"


def test_remove_persists_to_disk(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://example.com", "tok1")
    cfg.add("alias2", "https://other.example.com", "tok2")
    cfg.remove("alias1")

    cfg2 = ConnectionConfig(config_dir=tmp_path)
    cfg2.load()
    assert "alias1" not in cfg2.connections
    assert "alias2" in cfg2.connections


# ---------------------------------------------------------------------------
# List connections
# ---------------------------------------------------------------------------


def test_list_connections_returns_all(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://first.example.com", "tok1")
    cfg.add("alias2", "https://second.example.com", "tok2")
    items = cfg.list_connections()
    assert len(items) == 2


def test_list_connections_includes_active_flag(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.add("alias1", "https://first.example.com", "tok1")
    cfg.add("alias2", "https://second.example.com", "tok2")
    items = cfg.list_connections()
    active_items = [i for i in items if i["active"]]
    assert len(active_items) == 1
    assert active_items[0]["alias"] == "alias1"


def test_list_connections_empty(tmp_path):
    cfg = ConnectionConfig(config_dir=tmp_path)
    assert cfg.list_connections() == []


def test_load_invalid_connections_type(tmp_path):
    """Structural validation: connections must be a dict."""
    config_file = tmp_path / "config.json"
    config_file.write_text('{"active": null, "connections": "not-a-dict"}')
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(InvalidConfigError, match="must be a JSON object"):
        cfg.load()


def test_load_invalid_active_type(tmp_path):
    """Structural validation: active must be string or null."""
    config_file = tmp_path / "config.json"
    config_file.write_text('{"active": 123, "connections": {}}')
    cfg = ConnectionConfig(config_dir=tmp_path)
    with pytest.raises(InvalidConfigError, match="must be a string"):
        cfg.load()


def test_url_validation_rejects_invalid_scheme(tmp_path):
    """URL must start with http:// or https://."""
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.load()
    with pytest.raises(InvalidURLError):
        cfg.add("test", "ftp://example.com", "token")


def test_url_validation_rejects_bare_hostname(tmp_path):
    """URL must start with http:// or https://."""
    cfg = ConnectionConfig(config_dir=tmp_path)
    cfg.load()
    with pytest.raises(InvalidURLError):
        cfg.add("test", "example.com:8081", "token")
