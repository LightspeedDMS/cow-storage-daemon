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
        # NOTE: do NOT create the mutex here. In Python 3.9 asyncio.Lock() binds to
        # the loop returned by get_event_loop() AT CREATION TIME. CloneManager is
        # constructed before/outside uvicorn's serving loop, so a mutex created here
        # would bind to the wrong loop and raise "got Future ... attached to a
        # different loop" under CONTENDED _get_source_lock (concurrent creates).
        # It is created lazily inside the running loop via _get_source_locks_mutex(),
        # which rebinds the mutex if the running loop has changed since creation.
        self._source_locks_mutex: Optional[asyncio.Lock] = None
        self._source_locks_mutex_loop: Optional[asyncio.AbstractEventLoop] = None

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

    def _validate_dest_path(self, dest_path: str) -> str:
        """Validate dest_path is non-empty and resolves under storage_path.

        Uses Path.resolve() before comparison so symlinks, '..' traversal,
        and double slashes are all defeated (mirrors _validate_source_path pattern).

        Returns the resolved absolute path string if valid.
        Raises PathNotAllowedError if empty or outside storage_path.
        """
        if not dest_path:
            raise PathNotAllowedError("dest_path must not be empty")

        resolved = Path(dest_path).resolve()
        storage_root = Path(self._base_path).resolve()
        try:
            resolved.relative_to(storage_root)
        except ValueError:
            raise PathNotAllowedError(
                f"dest_path must be under storage_path: {dest_path}"
            )
        return str(resolved)

    def _get_source_locks_mutex(self) -> asyncio.Lock:
        """Return the LRU-map mutex bound to the CURRENTLY running event loop.

        Created lazily (not in __init__) and REBOUND if the running loop has changed
        since creation -- the daemon constructs the manager on the throwaway
        ``asyncio.run`` loop in __main__ but serves on the uvicorn loop, and a mutex
        bound to the construction loop raises "got Future ... attached to a different
        loop" under CONTENDED _get_source_lock (concurrent creates -> HTTP 500).

        On a loop change the per-source lock map is also cleared: every asyncio.Lock
        stored in it was bound to the old loop and must not be reused on the new loop.
        """
        running_loop = asyncio.get_running_loop()
        if (
            self._source_locks_mutex is None
            or self._source_locks_mutex_loop is not running_loop
        ):
            self._source_locks_mutex = asyncio.Lock()
            self._source_locks_mutex_loop = running_loop
            # Per-source locks belonged to the previous loop -- discard them so new
            # ones are created bound to the running loop.
            self._source_locks.clear()
        return self._source_locks_mutex

    async def _get_source_lock(self, source_path: str) -> asyncio.Lock:
        """Return (creating if needed) the asyncio.Lock for a given source_path.

        Uses LRU eviction to bound the map to SOURCE_LOCK_MAX entries.
        """
        async with self._get_source_locks_mutex():
            if source_path in self._source_locks:
                self._source_locks.move_to_end(source_path)
                return self._source_locks[source_path]
            lock = asyncio.Lock()
            self._source_locks[source_path] = lock
            if len(self._source_locks) > self.SOURCE_LOCK_MAX:
                self._source_locks.popitem(last=False)
            return lock

    async def submit_clone_job(
        self,
        source_path: str,
        namespace: str,
        name: str,
        dest_path: Optional[str] = None,
    ) -> str:
        """Submit a clone creation job. Returns job_id immediately (AC3).

        Raises ConflictError if a clone with namespace+name already exists.
        Raises PathNotAllowedError if source_path is not under allowed roots,
        or if dest_path is provided but resolves outside storage_path.
        The actual clone runs in a background asyncio task.

        When dest_path is provided, the clone is placed at that resolved path
        and stored in metadata. When None, the legacy {base}/{namespace}/{name}
        layout is used (backward compatible).
        """
        self._validate_source_path(source_path)

        validated_dest: Optional[str] = None
        if dest_path is not None:
            validated_dest = self._validate_dest_path(dest_path)

        if await self._store.clone_exists(namespace, name):
            raise ConflictError(
                f"Clone '{namespace}/{name}' already exists"
            )

        job_id = await self._store.create_job(
            namespace=namespace, name=name, source_path=source_path
        )

        # Launch background task without waiting for it
        asyncio.create_task(
            self._run_clone_job(job_id, source_path, namespace, name, validated_dest)
        )
        return job_id

    async def _run_clone_job(
        self,
        job_id: str,
        source_path: str,
        namespace: str,
        name: str,
        dest_path: Optional[str] = None,
    ) -> None:
        """Execute the clone operation in the background (AC3, AC7).

        When dest_path is provided (already validated and resolved), the clone
        is placed there. Otherwise the legacy {base}/{namespace}/{name} layout
        is used for full backward compatibility.
        """
        await self._store.update_job_status(job_id, "running")

        if dest_path is not None:
            actual_dest = Path(dest_path)
        else:
            actual_dest = Path(self._base_path) / namespace / name
        actual_dest.parent.mkdir(parents=True, exist_ok=True)

        source_lock = await self._get_source_lock(source_path)
        async with source_lock:
            try:
                await filesystem.perform_reflink_copy(source_path, str(actual_dest))

                # FIX 5: compute actual directory size after clone
                size_bytes = await asyncio.to_thread(_get_dir_size, str(actual_dest))

                clone_path = str(actual_dest)
                await self._store.save_clone(
                    namespace=namespace,
                    name=name,
                    source_path=source_path,
                    clone_path=clone_path,
                    size_bytes=size_bytes,
                    dest_path=dest_path,
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
        """Delete a clone's directory and metadata. Returns True if deleted.

        Reads the actual path from metadata (dest_path if set, else the legacy
        {base}/{namespace}/{name} layout). Raises on rmtree errors instead of
        silently swallowing them — silent failure was the root cause of disk leaks
        for dest_path-based clones (Codex B1).
        """
        import logging
        import shutil

        logger = logging.getLogger(__name__)

        clone = await self._store.get_clone(namespace, name)
        if clone is None:
            return False

        # Determine the actual directory to remove from metadata
        stored_dest = clone.get("dest_path")
        if stored_dest:
            clone_dir = Path(stored_dest)
        else:
            clone_dir = Path(self._base_path) / namespace / name

        if clone_dir.exists():
            try:
                await asyncio.to_thread(shutil.rmtree, str(clone_dir))
            except Exception as exc:
                logger.error(
                    "Failed to remove clone directory %s for %s/%s: %s",
                    clone_dir,
                    namespace,
                    name,
                    exc,
                )
                raise

        return await self._store.delete_clone(namespace, name)

    async def list_clones(self, namespace: Optional[str] = None) -> List[Dict]:
        """List all clones, optionally filtered by namespace."""
        return await self._store.list_clones(namespace=namespace)
