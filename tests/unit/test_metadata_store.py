"""Tests for SQLite metadata store (AC6)."""

import asyncio

import pytest

from cow_storage_daemon.core.metadata_store import MetadataStore


@pytest.fixture
async def store(tmp_path):
    """Provide a fresh MetadataStore backed by a temp DB."""
    db_path = tmp_path / ".cow-daemon.db"
    s = MetadataStore(str(db_path))
    await s.initialize()
    yield s
    await s.close()


class TestMetadataStoreInitialization:
    """Tests for store initialization."""

    async def test_initialize_creates_tables(self, tmp_path):
        """Initializing store creates clones and jobs tables."""
        db_path = tmp_path / "test.db"
        store = MetadataStore(str(db_path))
        await store.initialize()
        clones = await store.list_clones()
        assert clones == []
        await store.close()

    async def test_initialize_sets_wal_mode(self, tmp_path):
        """Database should be in WAL journal mode."""
        import aiosqlite
        db_path = tmp_path / "test.db"
        store = MetadataStore(str(db_path))
        await store.initialize()
        async with aiosqlite.connect(str(db_path)) as db:
            async with db.execute("PRAGMA journal_mode") as cursor:
                row = await cursor.fetchone()
        assert row[0] == "wal"
        await store.close()

    async def test_initialize_is_idempotent(self, tmp_path):
        """Calling initialize twice should not raise."""
        db_path = tmp_path / "test.db"
        store = MetadataStore(str(db_path))
        await store.initialize()
        await store.initialize()
        await store.close()


class TestCloneCRUD:
    """Tests for clone metadata CRUD operations."""

    async def test_save_clone(self, store):
        """Saving a clone record persists it."""
        await store.save_clone(
            namespace="cidx",
            name="clone-001",
            source_path="/data/repo",
            clone_path="cidx/clone-001",
            size_bytes=1024,
        )
        clone = await store.get_clone("cidx", "clone-001")
        assert clone is not None
        assert clone["namespace"] == "cidx"
        assert clone["name"] == "clone-001"
        assert clone["source_path"] == "/data/repo"
        assert clone["clone_path"] == "cidx/clone-001"
        assert clone["size_bytes"] == 1024

    async def test_save_clone_records_created_at(self, store):
        """Saved clone should have a created_at timestamp."""
        await store.save_clone(
            namespace="ns",
            name="clone",
            source_path="/data",
            clone_path="ns/clone",
            size_bytes=0,
        )
        clone = await store.get_clone("ns", "clone")
        assert clone["created_at"] is not None

    async def test_get_clone_returns_none_if_not_found(self, store):
        """Getting non-existent clone returns None."""
        result = await store.get_clone("ns", "nonexistent")
        assert result is None

    async def test_delete_clone(self, store):
        """Deleting a clone removes it from the store."""
        await store.save_clone(
            namespace="ns",
            name="clone",
            source_path="/data",
            clone_path="ns/clone",
            size_bytes=0,
        )
        await store.delete_clone("ns", "clone")
        result = await store.get_clone("ns", "clone")
        assert result is None

    async def test_delete_nonexistent_clone_returns_false(self, store):
        """Deleting a non-existent clone returns False."""
        result = await store.delete_clone("ns", "nonexistent")
        assert result is False

    async def test_delete_existing_clone_returns_true(self, store):
        """Deleting an existing clone returns True."""
        await store.save_clone(
            namespace="ns",
            name="clone",
            source_path="/data",
            clone_path="ns/clone",
            size_bytes=0,
        )
        result = await store.delete_clone("ns", "clone")
        assert result is True

    async def test_list_clones_empty(self, store):
        """List clones returns empty list when no clones exist."""
        result = await store.list_clones()
        assert result == []

    async def test_list_clones_all(self, store):
        """List clones returns all clones."""
        await store.save_clone("ns1", "c1", "/d1", "ns1/c1", 100)
        await store.save_clone("ns1", "c2", "/d2", "ns1/c2", 200)
        await store.save_clone("ns2", "c3", "/d3", "ns2/c3", 300)
        result = await store.list_clones()
        assert len(result) == 3

    async def test_list_clones_by_namespace(self, store):
        """List clones filtered by namespace."""
        await store.save_clone("ns1", "c1", "/d1", "ns1/c1", 100)
        await store.save_clone("ns1", "c2", "/d2", "ns1/c2", 200)
        await store.save_clone("ns2", "c3", "/d3", "ns2/c3", 300)
        result = await store.list_clones(namespace="ns1")
        assert len(result) == 2
        for c in result:
            assert c["namespace"] == "ns1"

    async def test_clone_exists(self, store):
        """clone_exists returns True/False correctly."""
        await store.save_clone("ns", "clone", "/data", "ns/clone", 0)
        assert await store.clone_exists("ns", "clone") is True
        assert await store.clone_exists("ns", "other") is False

    async def test_count_by_namespace(self, store):
        """count_by_namespace returns correct namespace counts."""
        await store.save_clone("ns1", "c1", "/d1", "ns1/c1", 0)
        await store.save_clone("ns1", "c2", "/d2", "ns1/c2", 0)
        await store.save_clone("ns2", "c3", "/d3", "ns2/c3", 0)
        counts = await store.count_by_namespace()
        assert counts["ns1"] == 2
        assert counts["ns2"] == 1

    async def test_total_clone_count(self, store):
        """total_clone_count returns correct total."""
        await store.save_clone("ns1", "c1", "/d1", "ns1/c1", 0)
        await store.save_clone("ns2", "c2", "/d2", "ns2/c2", 0)
        count = await store.total_clone_count()
        assert count == 2


class TestJobTracking:
    """Tests for job tracking in the metadata store."""

    async def test_create_job(self, store):
        """Creating a job persists it with pending status."""
        job_id = await store.create_job(
            namespace="ns",
            name="clone",
            source_path="/data",
        )
        assert job_id is not None
        job = await store.get_job(job_id)
        assert job is not None
        assert job["status"] == "pending"
        assert job["namespace"] == "ns"
        assert job["name"] == "clone"
        assert job["source_path"] == "/data"

    async def test_update_job_status_to_running(self, store):
        """Updating job to running status."""
        job_id = await store.create_job("ns", "clone", "/data")
        await store.update_job_status(job_id, "running")
        job = await store.get_job(job_id)
        assert job["status"] == "running"

    async def test_update_job_status_to_completed(self, store):
        """Completing job sets status and clone_path."""
        job_id = await store.create_job("ns", "clone", "/data")
        await store.update_job_status(job_id, "completed", clone_path="ns/clone")
        job = await store.get_job(job_id)
        assert job["status"] == "completed"
        assert job["clone_path"] == "ns/clone"
        assert job["completed_at"] is not None

    async def test_update_job_status_to_failed(self, store):
        """Failing job sets status and error message."""
        job_id = await store.create_job("ns", "clone", "/data")
        await store.update_job_status(job_id, "failed", error="Source not found")
        job = await store.get_job(job_id)
        assert job["status"] == "failed"
        assert job["error"] == "Source not found"
        assert job["completed_at"] is not None

    async def test_get_nonexistent_job_returns_none(self, store):
        """Getting a non-existent job returns None."""
        result = await store.get_job("nonexistent-job-id")
        assert result is None

    async def test_job_id_is_unique(self, store):
        """Each job creation produces a unique job_id."""
        id1 = await store.create_job("ns", "c1", "/d1")
        id2 = await store.create_job("ns", "c2", "/d2")
        assert id1 != id2


class TestConcurrentWrites:
    """Tests for asyncio.Lock protecting writes (AC6)."""

    async def test_concurrent_saves_do_not_corrupt(self, store):
        """Concurrent clone saves should all succeed without corruption."""
        async def save_clone(i):
            await store.save_clone(
                namespace="ns",
                name=f"clone-{i}",
                source_path=f"/data/{i}",
                clone_path=f"ns/clone-{i}",
                size_bytes=i * 100,
            )

        await asyncio.gather(*[save_clone(i) for i in range(20)])
        clones = await store.list_clones()
        assert len(clones) == 20

    async def test_store_has_write_lock(self, store):
        """MetadataStore should expose a write lock."""
        assert hasattr(store, "_write_lock")
        assert isinstance(store._write_lock, asyncio.Lock)
