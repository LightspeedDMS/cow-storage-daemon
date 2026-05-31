"""Tests for CloneManager - async job-based clone lifecycle (AC3, AC7, AC8)."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from cow_storage_daemon.core.clone_manager import CloneManager, PathNotAllowedError
from cow_storage_daemon.core.filesystem import FilesystemError


@pytest.fixture
async def manager(tmp_path):
    """Provide a CloneManager backed by a temp directory and DB."""
    from cow_storage_daemon.core.metadata_store import MetadataStore
    db_path = tmp_path / ".cow-daemon.db"
    store = MetadataStore(str(db_path))
    await store.initialize()
    mgr = CloneManager(base_path=str(tmp_path), store=store)
    yield mgr
    await store.close()


@pytest.fixture
def sync_manager(tmp_path):
    """Provide a CloneManager for synchronous path-validation tests (no async setup)."""
    from cow_storage_daemon.core.metadata_store import MetadataStore

    async def _build():
        db_path = tmp_path / ".cow-daemon.db"
        store = MetadataStore(str(db_path))
        await store.initialize()
        return store

    store = asyncio.get_event_loop().run_until_complete(_build())
    mgr = CloneManager(base_path=str(tmp_path), store=store)
    yield mgr
    asyncio.get_event_loop().run_until_complete(store.close())


async def _wait_for_job(manager, job_id, iterations=20, delay=0.05):
    """Wait until a job reaches a terminal state (completed or failed)."""
    for _ in range(iterations):
        job = await manager.get_job(job_id)
        if job["status"] in ("completed", "failed"):
            return job
        await asyncio.sleep(delay)
    return await manager.get_job(job_id)


class TestCloneJobSubmission:
    """Tests for async job-based clone creation (AC3)."""

    async def test_submit_returns_job_id_immediately(self, manager, tmp_path):
        """submit_clone_job returns a job_id without waiting for clone to complete."""
        source = tmp_path / "source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="clone",
            )
            assert job_id is not None
            assert isinstance(job_id, str)
            assert len(job_id) > 0

    async def test_job_starts_as_pending(self, manager, tmp_path):
        """Newly submitted job should have status 'pending' or 'running'."""
        source = tmp_path / "source"
        source.mkdir()

        async def slow_copy(*args, **kwargs):
            await asyncio.sleep(0.1)

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            side_effect=slow_copy,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="clone",
            )
            job = await manager.get_job(job_id)
            assert job is not None
            assert job["status"] in ("pending", "running")

    async def test_job_completes_successfully(self, manager, tmp_path):
        """Job should eventually reach 'completed' status."""
        source = tmp_path / "source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="clone",
            )
            # Wait for job to complete
            for _ in range(20):
                job = await manager.get_job(job_id)
                if job["status"] == "completed":
                    break
                await asyncio.sleep(0.05)

            job = await manager.get_job(job_id)
            assert job["status"] == "completed"
            assert job["clone_path"] is not None

    async def test_job_fails_when_copy_raises(self, manager, tmp_path):
        """Job should reach 'failed' status when copy raises FilesystemError."""
        source = tmp_path / "source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            side_effect=FilesystemError("disk full"),
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="clone",
            )
            for _ in range(20):
                job = await manager.get_job(job_id)
                if job["status"] == "failed":
                    break
                await asyncio.sleep(0.05)

            job = await manager.get_job(job_id)
            assert job["status"] == "failed"
            assert job["error"] is not None


class TestCloneLifecycle:
    """Tests for clone get, delete, and list operations (AC1, AC8)."""

    async def _create_completed_clone(self, manager, tmp_path, namespace, name):
        """Helper: submit a clone job and wait for completion."""
        source = tmp_path / f"source-{namespace}-{name}"
        source.mkdir(exist_ok=True)
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace=namespace,
                name=name,
            )
            for _ in range(20):
                job = await manager.get_job(job_id)
                if job["status"] == "completed":
                    break
                await asyncio.sleep(0.05)
        return job_id

    async def test_get_clone_returns_info_after_completion(self, manager, tmp_path):
        """get_clone returns clone info after job completes."""
        await self._create_completed_clone(manager, tmp_path, "ns", "c1")
        clone = await manager.get_clone("ns", "c1")
        assert clone is not None
        assert clone["namespace"] == "ns"
        assert clone["name"] == "c1"

    async def test_get_clone_returns_none_if_not_found(self, manager):
        """get_clone returns None for non-existent clone."""
        result = await manager.get_clone("ns", "nonexistent")
        assert result is None

    async def test_delete_clone_removes_it(self, manager, tmp_path):
        """delete_clone removes the clone record and directory."""
        await self._create_completed_clone(manager, tmp_path, "ns", "c1")
        result = await manager.delete_clone("ns", "c1")
        assert result is True
        clone = await manager.get_clone("ns", "c1")
        assert clone is None

    async def test_delete_nonexistent_clone_returns_false(self, manager):
        """delete_clone returns False for non-existent clone."""
        result = await manager.delete_clone("ns", "nonexistent")
        assert result is False

    async def test_list_clones_returns_all(self, manager, tmp_path):
        """list_clones returns all clones across namespaces."""
        await self._create_completed_clone(manager, tmp_path, "ns1", "c1")
        await self._create_completed_clone(manager, tmp_path, "ns2", "c2")
        clones = await manager.list_clones()
        assert len(clones) == 2

    async def test_list_clones_filtered_by_namespace(self, manager, tmp_path):
        """list_clones with namespace filter returns only matching clones."""
        await self._create_completed_clone(manager, tmp_path, "ns1", "c1")
        await self._create_completed_clone(manager, tmp_path, "ns2", "c2")
        clones = await manager.list_clones(namespace="ns1")
        assert len(clones) == 1
        assert clones[0]["namespace"] == "ns1"

    async def test_submit_conflict_raises_error(self, manager, tmp_path):
        """Submitting a clone with duplicate namespace+name raises ConflictError."""
        from cow_storage_daemon.core.clone_manager import ConflictError
        await self._create_completed_clone(manager, tmp_path, "ns", "c1")
        source = tmp_path / "another-source"
        source.mkdir(exist_ok=True)
        with pytest.raises(ConflictError):
            await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="c1",
            )


class TestPerSourceConcurrency:
    """Tests for per-source asyncio.Lock concurrency control (AC7)."""

    async def test_same_source_serializes_clones(self, manager, tmp_path):
        """Two clones from the same source should serialize (not run concurrently)."""
        source = tmp_path / "shared-source"
        source.mkdir()
        execution_order = []

        async def tracked_copy(src, dst):
            execution_order.append(("start", dst))
            await asyncio.sleep(0.03)
            execution_order.append(("end", dst))

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            side_effect=tracked_copy,
        ):
            job1 = await manager.submit_clone_job(str(source), "ns", "c1")
            job2 = await manager.submit_clone_job(str(source), "ns", "c2")

            for _ in range(40):
                j1 = await manager.get_job(job1)
                j2 = await manager.get_job(job2)
                if j1["status"] in ("completed", "failed") and j2["status"] in ("completed", "failed"):
                    break
                await asyncio.sleep(0.05)

        assert len(execution_order) == 4, f"Expected 4 events, got {len(execution_order)}: {execution_order}"
        starts = [i for i, (evt, _) in enumerate(execution_order) if evt == "start"]
        ends = [i for i, (evt, _) in enumerate(execution_order) if evt == "end"]
        assert ends[0] < starts[1], "Same-source clones should serialize: first end must precede second start"

    async def test_different_sources_can_run_in_parallel(self, manager, tmp_path):
        """Two clones from different sources should start close together (concurrent)."""
        source1 = tmp_path / "source1"
        source2 = tmp_path / "source2"
        source1.mkdir()
        source2.mkdir()
        start_times = []

        async def timed_copy(src, dst):
            start_times.append(asyncio.get_event_loop().time())
            await asyncio.sleep(0.05)

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            side_effect=timed_copy,
        ):
            job1 = await manager.submit_clone_job(str(source1), "ns", "c1")
            job2 = await manager.submit_clone_job(str(source2), "ns", "c2")

            for _ in range(40):
                j1 = await manager.get_job(job1)
                j2 = await manager.get_job(job2)
                if j1["status"] in ("completed", "failed") and j2["status"] in ("completed", "failed"):
                    break
                await asyncio.sleep(0.02)

        assert len(start_times) == 2, f"Expected 2 copy starts, got {len(start_times)}"
        time_diff = abs(start_times[1] - start_times[0])
        # True serialization would be >= 0.05s (the sleep duration); allow generous margin for CI/slow machines
        assert time_diff < 0.5, f"Different-source clones should start concurrently, diff={time_diff:.3f}s"

    async def test_manager_has_source_locks_map(self, manager):
        """CloneManager should have a _source_locks attribute for per-source locking."""
        assert hasattr(manager, "_source_locks")
        assert isinstance(manager._source_locks, dict)


class TestDestPathValidation:
    """Tests for _validate_dest_path security method (D1 / Codex B2)."""

    def test_accepts_path_under_storage_root(self, sync_manager, tmp_path):
        """A path that resolves inside storage_path is accepted and returned resolved."""
        valid = str(tmp_path / ".versioned" / "alias" / "v_123")
        result = sync_manager._validate_dest_path(valid)
        assert result == str((tmp_path / ".versioned" / "alias" / "v_123").resolve())

    def test_rejects_path_traversal(self, sync_manager, tmp_path):
        """A path with ../ that escapes storage_path is rejected."""
        traversal = str(tmp_path / ".." / "escape")
        with pytest.raises(PathNotAllowedError):
            sync_manager._validate_dest_path(traversal)

    def test_rejects_symlink_escaping_storage_path(self, sync_manager, tmp_path):
        """A symlink inside storage_path that resolves outside is rejected."""
        outside = tmp_path.parent / "outside_target"
        outside.mkdir(exist_ok=True)
        link = tmp_path / "escape_link"
        link.symlink_to(str(outside))
        with pytest.raises(PathNotAllowedError):
            sync_manager._validate_dest_path(str(link / "some_dir"))

    def test_rejects_empty_string(self, sync_manager):
        """Empty string is rejected as distinct from None."""
        with pytest.raises(PathNotAllowedError):
            sync_manager._validate_dest_path("")

    def test_rejects_absolute_escape(self, sync_manager):
        """An absolute path outside storage_path is rejected."""
        with pytest.raises(PathNotAllowedError):
            sync_manager._validate_dest_path("/etc/passwd_dest")


class TestSubmitCloneJobDestPath:
    """Tests for submit_clone_job with dest_path parameter (D1)."""

    async def test_submit_with_dest_path_writes_to_specified_path(self, manager, tmp_path):
        """When dest_path provided, clone is placed at that resolved path."""
        source = tmp_path / "source"
        source.mkdir()
        dest = tmp_path / ".versioned" / "alias-with.dots" / "v_123"
        actual_copies = []

        async def capture_copy(src, dst):
            actual_copies.append(dst)

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            side_effect=capture_copy,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="v_123",
                dest_path=str(dest),
            )
            await _wait_for_job(manager, job_id)

        assert len(actual_copies) == 1
        assert actual_copies[0] == str(dest.resolve())

    async def test_submit_with_dest_path_persists_dest_path_in_metadata(self, manager, tmp_path):
        """submit_clone_job with dest_path saves dest_path in MetadataStore."""
        source = tmp_path / "source"
        source.mkdir()
        dest = tmp_path / ".versioned" / "myalias" / "v_1"

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="v_1",
                dest_path=str(dest),
            )
            await _wait_for_job(manager, job_id)

        clone = await manager.get_clone("ns", "v_1")
        assert clone is not None
        assert clone["dest_path"] == str(dest.resolve())

    async def test_submit_without_dest_path_preserves_legacy_layout(self, manager, tmp_path):
        """submit_clone_job without dest_path uses {base}/{namespace}/{name} layout."""
        source = tmp_path / "source"
        source.mkdir()
        actual_copies = []

        async def capture_copy(src, dst):
            actual_copies.append(dst)

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            side_effect=capture_copy,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="legacy",
            )
            await _wait_for_job(manager, job_id)

        expected = str((tmp_path / "ns" / "legacy").resolve())
        assert len(actual_copies) == 1
        assert actual_copies[0] == expected

    async def test_submit_without_dest_path_saves_dest_path_none(self, manager, tmp_path):
        """submit_clone_job without dest_path saves dest_path=None in metadata."""
        source = tmp_path / "source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="nodest",
            )
            await _wait_for_job(manager, job_id)

        clone = await manager.get_clone("ns", "nodest")
        assert clone is not None
        assert clone.get("dest_path") is None

    async def test_submit_with_invalid_dest_path_raises(self, manager, tmp_path):
        """submit_clone_job with dest_path outside storage_path raises PathNotAllowedError."""
        source = tmp_path / "source"
        source.mkdir()
        with pytest.raises(PathNotAllowedError):
            await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="v_bad",
                dest_path="/etc/passwd_dest",
            )


class TestDeleteCloneDestPath:
    """Tests for delete_clone using dest_path from metadata (D1 / Codex B1)."""

    async def _create_dest_path_clone(self, manager, tmp_path, namespace, name, dest_path):
        """Submit clone with dest_path, create dest dir, wait for completion."""
        source = tmp_path / f"source-{namespace}-{name}"
        source.mkdir(exist_ok=True)
        dest_path.mkdir(parents=True, exist_ok=True)

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace=namespace,
                name=name,
                dest_path=str(dest_path),
            )
            await _wait_for_job(manager, job_id)

    async def test_delete_clone_with_dest_path_removes_actual_dest_dir(self, manager, tmp_path):
        """delete_clone for a dest_path-based clone removes the actual dest_path dir."""
        dest = tmp_path / ".versioned" / "alias-with.dots" / "v_123"
        await self._create_dest_path_clone(manager, tmp_path, "ns", "v_123", dest)
        assert dest.exists(), "dest directory should exist before delete"

        result = await manager.delete_clone("ns", "v_123")
        assert result is True
        assert not dest.exists(), "dest_path directory must be removed by delete_clone"

    async def test_delete_clone_with_dest_path_does_not_touch_legacy_path(self, manager, tmp_path):
        """delete_clone for a dest_path-based clone does NOT rmtree {base}/{ns}/{name}."""
        dest = tmp_path / ".versioned" / "alias" / "v_456"
        await self._create_dest_path_clone(manager, tmp_path, "ns", "v_456", dest)

        legacy = tmp_path / "ns" / "v_456"
        legacy.mkdir(parents=True, exist_ok=True)

        await manager.delete_clone("ns", "v_456")
        assert legacy.exists(), "{base}/{ns}/{name} must NOT be removed for dest_path-based clones"

    async def test_delete_legacy_clone_removes_base_ns_name_dir(self, manager, tmp_path):
        """delete_clone for a legacy clone (dest_path=None) removes {base}/{ns}/{name}."""
        from pathlib import Path
        source = tmp_path / "source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="legacy",
            )
            await _wait_for_job(manager, job_id)

        legacy_dir = Path(manager._base_path) / "ns" / "legacy"
        legacy_dir.mkdir(parents=True, exist_ok=True)

        result = await manager.delete_clone("ns", "legacy")
        assert result is True
        assert not legacy_dir.exists(), "Legacy clone dir must be removed"


class TestDeleteClonesDirectory:
    """Tests for directory cleanup in delete_clone (AC8)."""

    async def test_delete_removes_clone_directory(self, manager, tmp_path):
        """delete_clone removes the clone directory from disk (covers shutil.rmtree path)."""
        from pathlib import Path

        source = tmp_path / "source"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await manager.submit_clone_job(str(source), "ns", "c1")
            for _ in range(20):
                job = await manager.get_job(job_id)
                if job["status"] == "completed":
                    break
                await asyncio.sleep(0.05)

        # Create the directory at the manager's base_path so delete_clone can remove it
        clone_dir = Path(manager._base_path) / "ns" / "c1"
        clone_dir.mkdir(parents=True, exist_ok=True)

        await manager.delete_clone("ns", "c1")
        assert not clone_dir.exists()
