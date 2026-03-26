"""ConnectionConfig - manages ~/.cow-storage/config.json.

All writes are atomic: write to temp file in same directory, then os.replace().
File permissions are set to 0o600 after every write.
"""
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, List, Optional


class AliasValidationError(ValueError):
    """Raised when an alias fails validation."""


class DuplicateAliasError(ValueError):
    """Raised when attempting to register an alias that already exists."""


class InvalidConfigError(ValueError):
    """Raised when the config file contains invalid/unparseable content."""


class InvalidURLError(ValueError):
    """Raised when a URL doesn't have a valid scheme."""


_ALIAS_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

_DEFAULT_CONFIG_DIR = Path.home() / ".cow-storage"


class ConnectionConfig:
    """Manages connection configuration stored in a JSON file.

    Args:
        config_dir: Directory containing config.json. Defaults to ~/.cow-storage/.
    """

    def __init__(self, config_dir: Optional[Path] = None) -> None:
        self.config_dir = Path(config_dir) if config_dir is not None else _DEFAULT_CONFIG_DIR
        self._config_file = self.config_dir / "config.json"
        self.active: Optional[str] = None
        self.connections: Dict[str, Dict[str, str]] = {}

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load config from disk. Missing file → empty state. Corrupt JSON → InvalidConfigError."""
        if not self._config_file.exists():
            self.active = None
            self.connections = {}
            return
        raw = self._config_file.read_text(encoding="utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise InvalidConfigError(f"Config file is not valid JSON: {exc}") from exc
        self.active = data.get("active")
        self.connections = data.get("connections", {})
        # Structural validation
        if not isinstance(self.connections, dict):
            raise InvalidConfigError("Config 'connections' must be a JSON object.")
        if self.active is not None and not isinstance(self.active, str):
            raise InvalidConfigError("Config 'active' must be a string or null.")

    # ------------------------------------------------------------------
    # Save (atomic)
    # ------------------------------------------------------------------

    def save(self) -> None:
        """Persist current state to disk atomically with chmod 600."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(str(self.config_dir), 0o700)
        data = {
            "active": self.active,
            "connections": self.connections,
        }
        content = json.dumps(data, indent=2)
        # Write to a temp file in the same directory so os.replace() is atomic
        fd, tmp_path = tempfile.mkstemp(dir=self.config_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(content)
            os.chmod(tmp_path, 0o600)
            os.replace(tmp_path, str(self._config_file))
        except Exception:
            # Clean up temp file on any failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        # Ensure permissions on final file (os.replace preserves source perms on Linux)
        os.chmod(str(self._config_file), 0o600)

    # ------------------------------------------------------------------
    # Alias validation / URL normalisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_alias(alias: str) -> None:
        if not alias or not _ALIAS_RE.match(alias):
            raise AliasValidationError(
                f"Invalid alias {alias!r}: only alphanumeric characters, hyphens, "
                "and underscores are allowed."
            )

    @staticmethod
    def _normalize_url(url: str) -> str:
        if not url.startswith(("http://", "https://")):
            raise InvalidURLError(
                f"URL must begin with http:// or https://: {url!r}"
            )
        return url.rstrip("/")

    # ------------------------------------------------------------------
    # Mutating operations
    # ------------------------------------------------------------------

    def add(self, alias: str, url: str, token: str) -> None:
        """Register a new connection. Auto-activates if it is the first one."""
        self._validate_alias(alias)
        if alias in self.connections:
            raise DuplicateAliasError(f"Alias {alias!r} is already registered.")
        self.connections[alias] = {
            "url": self._normalize_url(url),
            "token": token,
        }
        if self.active is None:
            self.active = alias
        self.save()

    def activate(self, alias_or_url: str) -> None:
        """Set the active connection by alias or by URL. Raises KeyError if not found."""
        # Try alias match first
        if alias_or_url in self.connections:
            self.active = alias_or_url
            self.save()
            return
        # Try URL match
        try:
            normalized = self._normalize_url(alias_or_url)
        except InvalidURLError:
            raise KeyError(f"No connection found for alias or URL: {alias_or_url!r}")
        for alias, info in self.connections.items():
            if info["url"] == normalized:
                self.active = alias
                self.save()
                return
        raise KeyError(f"No connection found for alias or URL: {alias_or_url!r}")

    def update_token(self, alias: str, new_token: str) -> None:
        """Update the token for an existing connection. Raises KeyError if not found."""
        if alias not in self.connections:
            raise KeyError(f"Alias {alias!r} not found.")
        self.connections[alias]["token"] = new_token
        self.save()

    def remove(self, alias: str) -> None:
        """Remove a connection. Raises KeyError if not found. Sets active to None if it was active."""
        if alias not in self.connections:
            raise KeyError(f"Alias {alias!r} not found.")
        del self.connections[alias]
        if self.active == alias:
            self.active = None
        self.save()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def list_connections(self) -> List[Dict]:
        """Return list of dicts with keys: alias, url, token, active."""
        return [
            {
                "alias": alias,
                "url": info["url"],
                "token": info["token"],
                "active": alias == self.active,
            }
            for alias, info in self.connections.items()
        ]
