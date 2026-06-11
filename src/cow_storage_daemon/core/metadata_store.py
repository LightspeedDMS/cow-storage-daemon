"""SQLite metadata store for clone and job tracking (AC6).

Uses WAL journal mode and a single asyncio.Lock for all writes.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiosqlite


class MetadataStore:
    """Async SQLite store for clone metadata and job tracking.

    Write operations are serialized via a single asyncio.Lock (_write_lock).
    Journal mode is set to WAL at initialization for better read concurrency.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        # NOTE: do NOT create the asyncio.Lock here. In Python 3.9 asyncio.Lock()
        # binds to the event loop returned by get_event_loop() AT CREATION TIME.
        # This store is constructed before/outside uvicorn's serving loop, so a lock
        # created here would bind to the wrong loop -- uncontended acquire() works via
        # a fast path, but CONTENDED acquire() awaits a Future on the bound loop from
        # the serving loop and raises "got Future ... attached to a different loop"
        # (HTTP 500 on concurrent creates). The lock is created lazily inside the
        # running serving loop via _get_write_lock() instead.
        self._write_lock: Optional[asyncio.Lock] = None
        self._write_lock_loop: Optional[asyncio.AbstractEventLoop] = None
        self._db: Optional[aiosqlite.Connection] = None

    def _get_write_lock(self) -> asyncio.Lock:
        """Return the write lock bound to the CURRENTLY running event loop.

        Must be called from within an async path (a running loop). The lock is
        created lazily on first use and is REBOUND if the running loop has changed
        since it was created.

        Why rebind: the daemon constructs the store and runs initialize() inside the
        short-lived ``asyncio.run(_create())`` loop in __main__, then serves requests
        on a DIFFERENT uvicorn loop. A lock bound to the init loop raises
        "got Future ... attached to a different loop" the moment a CONTENDED acquire
        happens on the serving loop (the proven concurrent-create HTTP 500). Binding
        to the running loop -- and recreating on loop change -- guarantees the lock
        always belongs to the loop that is actually awaiting it.

        Safety: a rebind only replaces the lock object when no contended waiters can
        exist on the old loop, because acquire() on the old (now non-running) loop is
        impossible -- only the running loop can reach this code.
        """
        running_loop = asyncio.get_running_loop()
        if self._write_lock is None or self._write_lock_loop is not running_loop:
            self._write_lock = asyncio.Lock()
            self._write_lock_loop = running_loop
        return self._write_lock

    async def initialize(self) -> None:
        """Open the database connection, enable WAL mode, and create tables.

        The write lock is intentionally NOT pre-bound here: initialize() may run on a
        throwaway loop (see __main__), so the lock is created on first use within the
        serving loop via _get_write_lock(), which rebinds on loop change.
        """
        if self._db is not None:
            return
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._create_tables()

    async def close(self) -> None:
        """Close the database connection."""
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _create_tables(self) -> None:
        """Create clones and jobs tables if they do not exist, then migrate schema."""
        async with self._get_write_lock():
            await self._db.execute(
                """
                CREATE TABLE IF NOT EXISTS clones (
                    namespace TEXT NOT NULL,
                    name TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    clone_path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (namespace, name)
                )
                """
            )
            await self._db.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    namespace TEXT NOT NULL,
                    name TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    clone_path TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    error TEXT
                )
                """
            )
            await self._db.commit()
        # Run schema migrations after initial table creation
        await self._migrate_schema()

    async def _migrate_schema(self) -> None:
        """Apply backward-compatible schema migrations.

        Each migration is idempotent: it checks whether the change is needed
        before applying it, so running initialize() multiple times is safe.
        """
        async with self._get_write_lock():
            # Migration: add dest_path column if not present (D1 / Codex I1)
            async with self._db.execute("PRAGMA table_info(clones)") as cursor:
                columns = [row[1] async for row in cursor]
            if "dest_path" not in columns:
                await self._db.execute(
                    "ALTER TABLE clones ADD COLUMN dest_path TEXT"
                )
                await self._db.commit()

    # ------------------------------------------------------------------
    # Clone CRUD
    # ------------------------------------------------------------------

    async def save_clone(
        self,
        namespace: str,
        name: str,
        source_path: str,
        clone_path: str,
        size_bytes: int,
        dest_path: Optional[str] = None,
    ) -> None:
        """Persist a new clone record."""
        created_at = datetime.now(timezone.utc).isoformat()
        async with self._get_write_lock():
            await self._db.execute(
                """
                INSERT INTO clones
                    (namespace, name, source_path, clone_path, created_at, size_bytes, dest_path)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (namespace, name, source_path, clone_path, created_at, size_bytes, dest_path),
            )
            await self._db.commit()

    async def get_clone(self, namespace: str, name: str) -> Optional[Dict]:
        """Return clone record or None if not found."""
        async with self._db.execute(
            "SELECT * FROM clones WHERE namespace = ? AND name = ?",
            (namespace, name),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def delete_clone(self, namespace: str, name: str) -> bool:
        """Delete a clone record. Returns True if deleted, False if not found."""
        async with self._get_write_lock():
            cursor = await self._db.execute(
                "DELETE FROM clones WHERE namespace = ? AND name = ?",
                (namespace, name),
            )
            await self._db.commit()
        return cursor.rowcount > 0

    async def clone_exists(self, namespace: str, name: str) -> bool:
        """Return True if a clone with the given namespace+name exists."""
        async with self._db.execute(
            "SELECT 1 FROM clones WHERE namespace = ? AND name = ?",
            (namespace, name),
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None

    async def list_clones(self, namespace: Optional[str] = None) -> List[Dict]:
        """Return all clone records, optionally filtered by namespace."""
        if namespace is not None:
            query = "SELECT * FROM clones WHERE namespace = ? ORDER BY created_at"
            params = (namespace,)
        else:
            query = "SELECT * FROM clones ORDER BY created_at"
            params = ()
        async with self._db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def count_by_namespace(self) -> Dict[str, int]:
        """Return a mapping of namespace -> clone count."""
        async with self._db.execute(
            "SELECT namespace, COUNT(*) as cnt FROM clones GROUP BY namespace"
        ) as cursor:
            rows = await cursor.fetchall()
        return {row["namespace"]: row["cnt"] for row in rows}

    async def total_clone_count(self) -> int:
        """Return the total number of clones."""
        async with self._db.execute("SELECT COUNT(*) as cnt FROM clones") as cursor:
            row = await cursor.fetchone()
        return row["cnt"] if row else 0

    # ------------------------------------------------------------------
    # Job tracking
    # ------------------------------------------------------------------

    async def create_job(self, namespace: str, name: str, source_path: str) -> str:
        """Create a new job record in 'pending' state. Returns the job_id."""
        job_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        async with self._get_write_lock():
            await self._db.execute(
                """
                INSERT INTO jobs (job_id, status, namespace, name, source_path, created_at)
                VALUES (?, 'pending', ?, ?, ?, ?)
                """,
                (job_id, namespace, name, source_path, created_at),
            )
            await self._db.commit()
        return job_id

    async def get_job(self, job_id: str) -> Optional[Dict]:
        """Return job record or None if not found."""
        async with self._db.execute(
            "SELECT * FROM jobs WHERE job_id = ?",
            (job_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_job_status(
        self,
        job_id: str,
        status: str,
        clone_path: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update job status and optional clone_path or error fields."""
        completed_at = (
            datetime.now(timezone.utc).isoformat()
            if status in ("completed", "failed")
            else None
        )
        async with self._get_write_lock():
            await self._db.execute(
                """
                UPDATE jobs
                SET status = ?,
                    clone_path = COALESCE(?, clone_path),
                    error = COALESCE(?, error),
                    completed_at = COALESCE(?, completed_at)
                WHERE job_id = ?
                """,
                (status, clone_path, error, completed_at, job_id),
            )
            await self._db.commit()
