"""Health and statistics service for CoW Storage Daemon (AC10)."""

from __future__ import annotations

import time
from typing import Any, Dict

from cow_storage_daemon.core.filesystem import get_disk_stats, get_filesystem_type
from cow_storage_daemon.core.metadata_store import MetadataStore


class HealthService:
    """Provides health check and storage statistics."""

    def __init__(self, base_path: str, store: MetadataStore) -> None:
        self._base_path = base_path
        self._store = store
        self._start_time = time.monotonic()

    async def get_health(self) -> Dict[str, Any]:
        """Return health check data (AC10)."""
        disk = await get_disk_stats(self._base_path)
        fs_type = await get_filesystem_type(self._base_path)
        uptime = time.monotonic() - self._start_time
        return {
            "status": "healthy",
            "filesystem_type": fs_type,
            "cow_method": "reflink",
            "disk_total_bytes": disk["total_bytes"],
            "disk_used_bytes": disk["used_bytes"],
            "disk_available_bytes": disk["available_bytes"],
            "uptime_seconds": uptime,
        }

    async def get_stats(self) -> Dict[str, Any]:
        """Return storage statistics (AC10)."""
        disk = await get_disk_stats(self._base_path)
        total_count = await self._store.total_clone_count()
        by_namespace = await self._store.count_by_namespace()
        return {
            "disk_total_bytes": disk["total_bytes"],
            "disk_used_bytes": disk["used_bytes"],
            "disk_available_bytes": disk["available_bytes"],
            "clone_count_total": total_count,
            "clones_by_namespace": by_namespace,
        }
