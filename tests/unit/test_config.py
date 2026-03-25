"""Tests for DaemonConfig settings (config.py)."""

import pytest

from cow_storage_daemon.config import DaemonConfig


class TestDaemonConfig:
    """Tests for DaemonConfig Pydantic Settings."""

    def test_minimal_valid_config(self, tmp_path):
        """DaemonConfig accepts base_path and api_key as minimum required fields."""
        cfg = DaemonConfig(base_path=str(tmp_path), api_key="my-key")
        assert cfg.base_path == str(tmp_path)
        assert cfg.api_key == "my-key"

    def test_default_db_path_uses_base_path(self, tmp_path):
        """When db_path is not set, defaults to base_path/.cow-daemon.db."""
        cfg = DaemonConfig(base_path=str(tmp_path), api_key="key")
        assert cfg.db_path == str(tmp_path / ".cow-daemon.db")

    def test_explicit_db_path_overrides_default(self, tmp_path):
        """Explicitly set db_path is used as-is."""
        custom_db = str(tmp_path / "custom.db")
        cfg = DaemonConfig(base_path=str(tmp_path), api_key="key", db_path=custom_db)
        assert cfg.db_path == custom_db

    def test_health_requires_auth_defaults_to_false(self, tmp_path):
        """health_requires_auth defaults to False."""
        cfg = DaemonConfig(base_path=str(tmp_path), api_key="key")
        assert cfg.health_requires_auth is False

    def test_health_requires_auth_can_be_set_true(self, tmp_path):
        """health_requires_auth can be set to True."""
        cfg = DaemonConfig(base_path=str(tmp_path), api_key="key", health_requires_auth=True)
        assert cfg.health_requires_auth is True
