"""Tests for source path validation against allowed_source_roots (FIX 2)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from cow_storage_daemon.core.clone_manager import CloneManager, PathNotAllowedError
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
async def manager_with_roots(tmp_path, store):
    """CloneManager with allowed_source_roots set to tmp_path/allowed."""
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    mgr = CloneManager(
        base_path=str(tmp_path),
        store=store,
        allowed_source_roots=[str(allowed)],
    )
    yield mgr, allowed


@pytest.fixture
async def manager_no_roots(tmp_path, store):
    """CloneManager with empty allowed_source_roots (allow any)."""
    mgr = CloneManager(
        base_path=str(tmp_path),
        store=store,
        allowed_source_roots=[],
    )
    yield mgr


class TestAllowedSourceRoots:
    """Tests for source path validation against configured allowed roots (FIX 2)."""

    async def test_source_under_allowed_root_is_accepted(self, manager_with_roots, tmp_path):
        """Source path under an allowed root should be accepted."""
        mgr, allowed = manager_with_roots
        source = allowed / "repo"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await mgr.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="clone1",
            )
        assert job_id is not None

    async def test_source_outside_allowed_root_raises(self, manager_with_roots, tmp_path):
        """Source path outside allowed roots should raise PathNotAllowedError."""
        mgr, allowed = manager_with_roots
        outside = tmp_path / "outside"
        outside.mkdir()

        with pytest.raises(PathNotAllowedError):
            await mgr.submit_clone_job(
                source_path=str(outside),
                namespace="ns",
                name="clone2",
            )

    async def test_path_traversal_outside_allowed_root_rejected(self, manager_with_roots, tmp_path):
        """Path traversal that resolves outside allowed root should be rejected."""
        mgr, allowed = manager_with_roots
        # Create a dir inside allowed so the traversal prefix exists
        inner = allowed / "inner"
        inner.mkdir()
        # Construct a path that tries to escape via ../..
        traversal_path = str(inner) + "/../../outside"

        with pytest.raises(PathNotAllowedError):
            await mgr.submit_clone_job(
                source_path=traversal_path,
                namespace="ns",
                name="clone3",
            )

    async def test_empty_allowed_roots_accepts_any_path(self, manager_no_roots, tmp_path):
        """Empty allowed_source_roots means no restriction - any path accepted."""
        mgr = manager_no_roots
        source = tmp_path / "anywhere"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            job_id = await mgr.submit_clone_job(
                source_path=str(source),
                namespace="ns",
                name="clone4",
            )
        assert job_id is not None

    async def test_path_not_allowed_error_is_raised_before_job_created(
        self, manager_with_roots, tmp_path
    ):
        """PathNotAllowedError must be raised before any job is persisted."""
        mgr, allowed = manager_with_roots
        outside = tmp_path / "forbidden"
        outside.mkdir()

        with pytest.raises(PathNotAllowedError):
            await mgr.submit_clone_job(
                source_path=str(outside),
                namespace="ns",
                name="clone5",
            )

        # No job should have been created in the store
        jobs = await mgr._store.list_clones()
        assert len(jobs) == 0


class TestDaemonConfigAllowedSourceRoots:
    """Tests for allowed_source_roots field in DaemonConfig (FIX 2)."""

    def test_daemon_config_has_allowed_source_roots(self):
        """DaemonConfig must have allowed_source_roots field defaulting to empty list."""
        from cow_storage_daemon.config import DaemonConfig

        cfg = DaemonConfig(base_path="/tmp", api_key="key")
        assert hasattr(cfg, "allowed_source_roots")
        assert cfg.allowed_source_roots == []

    def test_daemon_config_accepts_allowed_source_roots(self, tmp_path):
        """DaemonConfig accepts a list of paths for allowed_source_roots."""
        from cow_storage_daemon.config import DaemonConfig

        cfg = DaemonConfig(
            base_path=str(tmp_path),
            api_key="key",
            allowed_source_roots=["/data/repos", "/mnt/storage"],
        )
        assert cfg.allowed_source_roots == ["/data/repos", "/mnt/storage"]
