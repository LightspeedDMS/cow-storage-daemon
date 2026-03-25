"""Configuration for CoW Storage Daemon.

Settings are loaded from config.json or from a dict passed to create_app().
Single API key in config; restart daemon to change key (AC2).
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings


class DaemonConfig(BaseSettings):
    """Runtime configuration for CoW Storage Daemon."""

    base_path: str
    api_key: str
    db_path: Optional[str] = None
    health_requires_auth: bool = False
    allowed_source_roots: List[str] = []  # Empty = allow any path (with startup warning)

    @field_validator("db_path", mode="before")
    @classmethod
    def default_db_path(cls, v: Optional[str], info) -> str:
        if v is not None:
            return v
        base = info.data.get("base_path", ".")
        return str(Path(base) / ".cow-daemon.db")

    model_config = {"env_prefix": "COW_DAEMON_", "extra": "ignore"}
