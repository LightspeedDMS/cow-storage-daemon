"""Tests for LRU-bounded source locks (FIX 3), create_task (FIX 4), size_bytes (FIX 5)."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from unittest.mock import AsyncMock, patch

import pytest

from cow_storage_daemon.core.clone_manager import CloneManager
from cow_storage_daemon.core.metadata_store import MetadataStore


@pytest.fixture
async def store(tmp_path):
    """Provide an initialized MetadataStore."""
    db_path = tmp_path / ".cow-daemon.db"
    s = MetadataStore(str(db_path))
    await s.initialize()
    yield s
    await s.close()


@pytest.fixture
async def manager(tmp_path, store):
    """CloneManager with default config."""
    return CloneManager(base_path=str(tmp_path), store=store)


class TestLRUBoundedSourceLocks:
    """Tests for LRU-bounded source lock map (FIX 3, AC7)."""

    async def test_source_locks_uses_ordered_dict(self, manager):
        """_source_locks must be an OrderedDict for LRU behavior."""
        assert isinstance(manager._source_locks, OrderedDict), (
            "_source_locks must be an OrderedDict, not a plain dict"
        )

    async def test_lock_map_bounded_at_max_size(self, tmp_path, store):
        """Lock map must not exceed SOURCE_LOCK_MAX entries."""
        small_max = 5
        mgr = CloneManager(
            base_path=str(tmp_path),
            store=store,
            source_lock_max=small_max,
        )

        # Access 10 distinct sources
        for i in range(10):
            await mgr._get_source_lock(f"/source/path/{i}")

        assert len(mgr._source_locks) <= small_max, (
            f"Lock map has {len(mgr._source_locks)} entries, expected <= {small_max}"
        )

    async def test_lru_eviction_removes_oldest(self, tmp_path, store):
        """LRU eviction removes the least-recently-used entry when limit exceeded."""
        small_max = 3
        mgr = CloneManager(
            base_path=str(tmp_path),
            store=store,
            source_lock_max=small_max,
        )

        # Access 3 sources to fill up
        await mgr._get_source_lock("/source/a")
        await mgr._get_source_lock("/source/b")
        await mgr._get_source_lock("/source/c")

        # Adding a 4th should evict /source/a (oldest/LRU)
        await mgr._get_source_lock("/source/d")

        assert "/source/a" not in mgr._source_locks, (
            "/source/a should have been evicted as the oldest entry"
        )
        assert "/source/d" in mgr._source_locks

    async def test_accessing_existing_key_moves_to_end(self, tmp_path, store):
        """Re-accessing an existing source moves it to the end (MRU position)."""
        small_max = 3
        mgr = CloneManager(
            base_path=str(tmp_path),
            store=store,
            source_lock_max=small_max,
        )

        await mgr._get_source_lock("/source/a")
        await mgr._get_source_lock("/source/b")
        await mgr._get_source_lock("/source/c")

        # Re-access /source/a - it should move to MRU position
        await mgr._get_source_lock("/source/a")

        # Adding /source/d should evict /source/b (now oldest)
        await mgr._get_source_lock("/source/d")

        assert "/source/b" not in mgr._source_locks, (
            "/source/b should have been evicted (was oldest after /source/a was re-accessed)"
        )
        assert "/source/a" in mgr._source_locks, (
            "/source/a should still be present (was re-accessed recently)"
        )

    async def test_evicted_source_gets_new_lock(self, tmp_path, store):
        """After eviction, accessing the evicted source creates a fresh lock."""
        small_max = 2
        mgr = CloneManager(
            base_path=str(tmp_path),
            store=store,
            source_lock_max=small_max,
        )

        lock_a_first = await mgr._get_source_lock("/source/a")
        await mgr._get_source_lock("/source/b")
        # This evicts /source/a
        await mgr._get_source_lock("/source/c")

        assert "/source/a" not in mgr._source_locks

        # Access /source/a again - should get a new lock object
        lock_a_second = await mgr._get_source_lock("/source/a")
        assert lock_a_second is not lock_a_first, (
            "Evicted source should receive a new lock object, not the old one"
        )

    async def test_default_source_lock_max_is_1024(self, manager):
        """Default SOURCE_LOCK_MAX should be 1024."""
        assert manager.SOURCE_LOCK_MAX == 1024


class TestCreateTaskUsed:
    """Tests verifying asyncio.create_task is used instead of ensure_future (FIX 4)."""

    async def test_submit_uses_create_task_not_ensure_future(self, tmp_path, store):
        """submit_clone_job must use asyncio.create_task, not asyncio.ensure_future."""
        import cow_storage_daemon.core.clone_manager as cm_module
        import inspect

        # Check submit_clone_job function source only (not module docstring)
        func_source = inspect.getsource(cm_module.CloneManager.submit_clone_job)
        assert "ensure_future" not in func_source, (
            "submit_clone_job must not use asyncio.ensure_future - use asyncio.create_task instead"
        )
        assert "create_task" in func_source, (
            "submit_clone_job must use asyncio.create_task"
        )


class TestActualSizeBytes:
    """Tests verifying actual directory size is computed after clone completes (FIX 5)."""

    async def test_size_bytes_nonzero_when_clone_has_files(self, tmp_path, store):
        """size_bytes in clone metadata should reflect actual file sizes after clone."""
        import os

        mgr = CloneManager(base_path=str(tmp_path), store=store)

        source = tmp_path / "source_with_data"
        source.mkdir()
        # Write some real data
        (source / "file1.txt").write_bytes(b"x" * 1024)
        (source / "file2.txt").write_bytes(b"y" * 2048)

        dest = tmp_path / "ns" / "sizeclone"
        dest.mkdir(parents=True)

        # Simulate what _run_clone_job does: copy source to dest, then compute size
        import shutil
        shutil.copytree(str(source), str(dest), dirs_exist_ok=True)

        async def fake_copy(src, dst):
            # Copy already done above
            pass

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            side_effect=fake_copy,
        ):
            job_id = await mgr.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="sizeclone",
            )
            for _ in range(30):
                job = await mgr.get_job(job_id)
                if job["status"] in ("completed", "failed"):
                    break
                await asyncio.sleep(0.05)

        clone = await mgr.get_clone("ns", "sizeclone")
        assert clone is not None
        assert clone["size_bytes"] > 0, (
            f"size_bytes should be non-zero after clone with real files, got {clone['size_bytes']}"
        )

    async def test_size_bytes_zero_for_empty_source(self, tmp_path, store):
        """size_bytes should be 0 for an empty source directory."""
        mgr = CloneManager(base_path=str(tmp_path), store=store)

        source = tmp_path / "empty_source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await mgr.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="emptyclone",
            )
            for _ in range(30):
                job = await mgr.get_job(job_id)
                if job["status"] in ("completed", "failed"):
                    break
                await asyncio.sleep(0.05)

        clone = await mgr.get_clone("ns", "emptyclone")
        assert clone is not None
        assert clone["size_bytes"] == 0
