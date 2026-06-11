"""Cross-loop asyncio.Lock regression tests (concurrent-create 500 bug).

Proven root cause (staging node .23): asyncio synchronization primitives created
at CONSTRUCTION time (in __init__) bind to whatever event loop get_event_loop()
returns at creation. The daemon constructs MetadataStore / CloneManager inside the
short-lived ``asyncio.run(_create())`` loop in ``__main__`` and then serves requests
on a DIFFERENT uvicorn loop. Uncontended acquire() takes a fast path (no Future) and
appears to work; a CONTENDED acquire() creates a Future on the lock's bound loop and
awaits it from the serving loop, raising:

    RuntimeError: ... got Future <Future pending> attached to a different loop

These tests reproduce that exact failure by binding the object on one loop and
exercising the CONTENDED lock from a second loop. They must FAIL on the buggy code
(RuntimeError) and PASS once each primitive is created lazily inside the running loop.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import patch

from cow_storage_daemon.core.clone_manager import CloneManager
from cow_storage_daemon.core.metadata_store import MetadataStore


def _run_on_dedicated_loop(coro_factory):
    """Run coro_factory() to completion on a brand-new loop in a worker thread.

    Returns (result, exception). The loop is created and closed inside the thread,
    so each call gets a genuinely distinct event loop -- which is what surfaces the
    cross-loop Future binding bug.
    """
    box: dict = {}

    def _worker() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            box["result"] = loop.run_until_complete(coro_factory())
        except BaseException as exc:  # noqa: BLE001 - we want to capture cross-loop RuntimeError
            box["exc"] = exc
        finally:
            loop.close()

    t = threading.Thread(target=_worker)
    t.start()
    t.join(timeout=30)
    return box.get("result"), box.get("exc")


class TestMetadataStoreCrossLoopWriteLock:
    """The write lock must bind to the serving loop, not the construction loop."""

    async def test_write_lock_bound_to_running_loop_after_initialize(self, tmp_path):
        """After initialize() the write lock is bound to the loop that ran initialize().

        We construct on the current test loop, initialize on the current loop, then
        verify a CONTENDED acquire on the current loop works (no cross-loop error).
        """
        store = MetadataStore(str(tmp_path / "loop.db"))
        await store.initialize()

        lock = store._get_write_lock() if hasattr(store, "_get_write_lock") else store._write_lock

        # Force contention: hold the lock, then have a second waiter park on a Future.
        await lock.acquire()
        waiter = asyncio.ensure_future(lock.acquire())
        await asyncio.sleep(0)  # let the waiter park (creates a Future on this loop)
        lock.release()
        await waiter  # must not raise cross-loop RuntimeError
        lock.release()
        await store.close()

    def test_metadata_store_write_lock_contended_across_loops(self, tmp_path):
        """Construct+initialize on loop A, then drive CONTENDED writes on loop B.

        On the buggy code the write lock is created in __init__ on loop A; the
        contended UPDATE/INSERT path on loop B awaits a Future attached to loop A ->
        RuntimeError. After the fix the lock is created lazily inside loop B's run,
        so concurrent writes succeed.
        """
        db_path = str(tmp_path / "xloop.db")

        # --- Loop A: construct + initialize the store (binds any __init__ locks to A)
        async def _build():
            s = MetadataStore(db_path)
            await s.initialize()
            return s

        store, build_exc = _run_on_dedicated_loop(_build)
        assert build_exc is None, f"build raised: {build_exc!r}"
        assert store is not None

        # --- Loop B: drive CONTENDED writes against the same store object
        async def _hammer():
            async def one(i: int) -> None:
                jid = await store.create_job("ns", f"c{i}", f"/src/{i}")
                await store.update_job_status(jid, "running")
                await store.update_job_status(jid, "completed", clone_path=f"ns/c{i}")

            # Many concurrent writers guarantee the single write lock is contended.
            await asyncio.gather(*[one(i) for i in range(25)])
            await store.close()

        _result, hammer_exc = _run_on_dedicated_loop(_hammer)
        assert hammer_exc is None, (
            "Contended writes across loops raised "
            f"{type(hammer_exc).__name__}: {hammer_exc} -- the write lock is bound to "
            "the construction loop, not the serving loop"
        )


class TestCloneManagerCrossLoopSourceMutex:
    """The per-source LRU mutex must bind to the serving loop, not construction loop."""

    def test_clone_manager_source_mutex_contended_across_loops(self, tmp_path):
        """Build store+manager on loop A, then contend _get_source_lock on loop B.

        _get_source_lock acquires self._source_locks_mutex. If that mutex is created
        in __init__ on loop A, a CONTENDED acquire on loop B raises the cross-loop
        RuntimeError. After the fix it is created lazily inside the running loop.
        """
        db_path = str(tmp_path / "mgr.db")

        async def _build():
            s = MetadataStore(db_path)
            await s.initialize()
            mgr = CloneManager(base_path=str(tmp_path), store=s)
            # Touch the source-locks mutex ON LOOP A so that, on the buggy code, the
            # mutex (created in __init__) is firmly bound to this construction loop.
            await mgr._get_source_lock("/warmup")
            return s, mgr

        built, build_exc = _run_on_dedicated_loop(_build)
        assert build_exc is None, f"build raised: {build_exc!r}"
        store, mgr = built

        async def _hammer():
            # Deterministically force CONTENTION on the single _source_locks_mutex:
            # grab it, park a second acquirer (creates a Future on the mutex's bound
            # loop), then release. On the buggy code the Future is attached to loop A
            # while we await it on loop B -> cross-loop RuntimeError.
            mutex = (
                mgr._get_source_locks_mutex()
                if hasattr(mgr, "_get_source_locks_mutex")
                else mgr._source_locks_mutex
            )
            await mutex.acquire()
            waiter = asyncio.ensure_future(mgr._get_source_lock("/src/contended"))
            await asyncio.sleep(0)  # let the waiter park on the mutex Future
            mutex.release()
            await waiter
            await store.close()

        _result, hammer_exc = _run_on_dedicated_loop(_hammer)
        assert hammer_exc is None, (
            "Contended _get_source_lock across loops raised "
            f"{type(hammer_exc).__name__}: {hammer_exc} -- the source-locks mutex is "
            "bound to the construction loop, not the serving loop"
        )


class TestConcurrentSubmitAcrossLoops:
    """End-to-end concurrency: many submit_clone_job calls across loops, zero 500s."""

    def test_concurrent_submit_clone_jobs_across_loops_no_500(self, tmp_path):
        """N concurrent submit_clone_job (mix same/different source) all succeed.

        Mirrors the production failure: store+manager built on the throwaway
        ``asyncio.run`` loop (loop A), requests served on the uvicorn loop (loop B).
        On the buggy code the contended write lock / source mutex raised the
        cross-loop RuntimeError, the route returned 500 and the background job died.
        After the fix every concurrent submit returns a job_id, no cross-loop error
        is raised, and every background job reaches a terminal status.

        Concurrency semantics are also asserted: clones of the SAME source serialize
        (per-source lock) while clones of DIFFERENT sources run in parallel.
        """
        # Three real source directories so reflink/path validation pass cleanly.
        sources = []
        for i in range(3):
            s = tmp_path / f"source-{i}"
            s.mkdir()
            (s / "data.txt").write_bytes(b"x" * 16)
            sources.append(str(s))

        async def _build():
            s = MetadataStore(str(tmp_path / "concurrent.db"))
            await s.initialize()
            mgr = CloneManager(base_path=str(tmp_path), store=s)
            return s, mgr

        built, build_exc = _run_on_dedicated_loop(_build)
        assert build_exc is None, f"build raised: {build_exc!r}"
        store, mgr = built

        # Track overlap per source to prove same-source serialization vs
        # different-source parallelism.
        active_per_source: dict = {src: 0 for src in sources}
        max_active_per_source: dict = {src: 0 for src in sources}
        max_active_total = {"v": 0}
        active_total = {"v": 0}
        overlap_lock = threading.Lock()

        async def fake_reflink(src: str, dst: str) -> None:
            # Record concurrency, then yield so overlapping clones interleave.
            with overlap_lock:
                active_per_source[src] += 1
                active_total["v"] += 1
                max_active_per_source[src] = max(max_active_per_source[src], active_per_source[src])
                max_active_total["v"] = max(max_active_total["v"], active_total["v"])
            await asyncio.sleep(0.05)
            with overlap_lock:
                active_per_source[src] -= 1
                active_total["v"] -= 1

        async def _hammer():
            # 12 jobs: 4 per source (forces same-source contention AND cross-source
            # parallelism), all submitted concurrently on the serving loop.
            specs = [(sources[i % 3], f"clone-{i}") for i in range(12)]

            with patch(
                "cow_storage_daemon.core.filesystem.perform_reflink_copy",
                side_effect=fake_reflink,
            ):
                job_ids = await asyncio.gather(
                    *[
                        mgr.submit_clone_job(source_path=src, namespace="ns", name=name)
                        for src, name in specs
                    ]
                )

                # Wait for all background jobs to reach a terminal state.
                statuses = {}
                for _ in range(100):
                    statuses = {}
                    for jid in job_ids:
                        job = await mgr.get_job(jid)
                        statuses[jid] = job["status"] if job else None
                    if all(s in ("completed", "failed") for s in statuses.values()):
                        break
                    await asyncio.sleep(0.05)

            await store.close()
            return job_ids, statuses

        result, hammer_exc = _run_on_dedicated_loop(_hammer)
        assert hammer_exc is None, (
            f"Concurrent submit raised {type(hammer_exc).__name__}: {hammer_exc} (cross-loop 500)"
        )
        job_ids, statuses = result

        # Every submit returned a unique, non-None job_id (zero 500s on submit).
        assert len(job_ids) == 12
        assert all(jid for jid in job_ids)
        assert len(set(job_ids)) == 12

        # Every background job completed successfully (reflink mocked, no errors).
        assert all(s == "completed" for s in statuses.values()), (
            f"some jobs did not complete: {statuses}"
        )

        # Same-source clones serialized: never more than one reflink active per source.
        for src in sources:
            assert max_active_per_source[src] == 1, (
                f"source {src} had {max_active_per_source[src]} concurrent reflinks -- "
                "per-source lock did not serialize"
            )

        # Different-source clones parallelized: more than one reflink active overall.
        assert max_active_total["v"] > 1, (
            "different-source clones did not run in parallel "
            f"(max total active = {max_active_total['v']})"
        )
