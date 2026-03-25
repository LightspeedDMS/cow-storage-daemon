"""Clone lifecycle manager with async job-based creation (AC3, AC7, AC8).

Provides submit_clone_job() which returns a job_id immediately and runs
the actual clone in the background. Per-source asyncio.Lock serializes
clones from the same source to avoid filesystem contention (AC7).
LRU-bounded lock map prevents unbounded memory growth (AC7, FIX 3).
"""

from __future__ import annotations

import asyncio
import os
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional

from cow_storage_daemon.core import filesystem
from cow_storage_daemon.core.metadata_store import MetadataStore


class ConflictError(Exception):
    """Raised when a clone with the same namespace+name already exists."""


class PathNotAllowedError(Exception):
    """Raised when source_path is not under any configured allowed_source_roots."""


def _get_dir_size(path: str) -> int:
    """Compute total size of all files under path using os.walk."""
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


class CloneManager:
    """Manages clone lifecycle: submit jobs, track status, get/delete clones.

    This is a dumb storage layer: it never auto-deletes clones (AC8).
    Clients are responsible for calling delete_clone() when done.
    """

    SOURCE_LOCK_MAX = 1024

    def __init__(
        self,
        base_path: str,
        store: MetadataStore,
        allowed_source_roots: Optional[List[str]] = None,
        source_lock_max: Optional[int] = None,
    ) -> None:
        self._base_path = base_path
        self._store = store
        self._allowed_source_roots: List[str] = allowed_source_roots or []
        if source_lock_max is not None:
            self.SOURCE_LOCK_MAX = source_lock_max
        self._source_locks: OrderedDict[str, asyncio.Lock] = OrderedDict()
        self._source_locks_mutex = asyncio.Lock()

    def _validate_source_path(self, source_path: str) -> None:
        """Validate source_path against allowed_source_roots.

        If allowed_source_roots is empty, all paths are allowed.
        Otherwise, resolved source_path must start with one of the allowed roots.
        Raises PathNotAllowedError if the path is not permitted.
        """
        if not self._allowed_source_roots:
            return

        resolved = Path(source_path).resolve()
        for root in self._allowed_source_roots:
            allowed_root = Path(root).resolve()
            try:
                resolved.relative_to(allowed_root)
                return  # Path is under this allowed root
            except ValueError:
                continue

        raise PathNotAllowedError(
            f"Source path '{source_path}' is not under any allowed source root: "
            f"{self._allowed_source_roots}"
        )

    async def _get_source_lock(self, source_path: str) -> asyncio.Lock:
        """Return (creating if needed) the asyncio.Lock for a given source_path.

        Uses LRU eviction to bound the map to SOURCE_LOCK_MAX entries.
        """
        async with self._source_locks_mutex:
            if source_path in self._source_locks:
                self._source_locks.move_to_end(source_path)
                return self._source_locks[source_path]
            lock = asyncio.Lock()
            self._source_locks[source_path] = lock
            if len(self._source_locks) > self.SOURCE_LOCK_MAX:
                self._source_locks.popitem(last=False)
            return lock

    async def submit_clone_job(
        self, source_path: str, namespace: str, name: str
    ) -> str:
        """Submit a clone creation job. Returns job_id immediately (AC3).

        Raises ConflictError if a clone with namespace+name already exists.
        Raises PathNotAllowedError if source_path is not under allowed roots.
        The actual clone runs in a background asyncio task.
        """
        self._validate_source_path(source_path)

        if await self._store.clone_exists(namespace, name):
            raise ConflictError(
                f"Clone '{namespace}/{name}' already exists"
            )

        job_id = await self._store.create_job(
            namespace=namespace, name=name, source_path=source_path
        )

        # Launch background task without waiting for it
        asyncio.create_task(
            self._run_clone_job(job_id, source_path, namespace, name)
        )
        return job_id

    async def _run_clone_job(
        self, job_id: str, source_path: str, namespace: str, name: str
    ) -> None:
        """Execute the clone operation in the background (AC3, AC7)."""
        await self._store.update_job_status(job_id, "running")

        dest_path = Path(self._base_path) / namespace / name
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        source_lock = await self._get_source_lock(source_path)
        async with source_lock:
            try:
                await filesystem.perform_reflink_copy(source_path, str(dest_path))

                # FIX 5: compute actual directory size after clone
                size_bytes = await asyncio.to_thread(_get_dir_size, str(dest_path))

                clone_path = f"{namespace}/{name}"
                await self._store.save_clone(
                    namespace=namespace,
                    name=name,
                    source_path=source_path,
                    clone_path=clone_path,
                    size_bytes=size_bytes,
                )
                await self._store.update_job_status(
                    job_id, "completed", clone_path=clone_path
                )
            except Exception as exc:
                await self._store.update_job_status(
                    job_id, "failed", error=str(exc)
                )

    async def get_job(self, job_id: str) -> Optional[Dict]:
        """Return job record or None if not found."""
        return await self._store.get_job(job_id)

    async def get_clone(self, namespace: str, name: str) -> Optional[Dict]:
        """Return clone info or None if not found."""
        return await self._store.get_clone(namespace, name)

    async def delete_clone(self, namespace: str, name: str) -> bool:
        """Delete a clone's directory and metadata. Returns True if deleted."""
        clone = await self._store.get_clone(namespace, name)
        if clone is None:
            return False

        # Remove the clone directory from disk
        clone_dir = Path(self._base_path) / namespace / name
        if clone_dir.exists():
            import shutil
            await asyncio.to_thread(shutil.rmtree, str(clone_dir), ignore_errors=True)

        return await self._store.delete_clone(namespace, name)

    async def list_clones(self, namespace: Optional[str] = None) -> List[Dict]:
        """List all clones, optionally filtered by namespace."""
        return await self._store.list_clones(namespace=namespace)
