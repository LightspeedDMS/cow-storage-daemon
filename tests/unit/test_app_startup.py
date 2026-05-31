"""Tests for app startup validation and lifespan management (FIX 1, FIX 6)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from cow_storage_daemon.core.filesystem import ReflinkNotSupportedError


async def _make_test_app(tmp_path):
    """Create a test app with reflink validation mocked out."""
    from cow_storage_daemon.app import create_app

    with patch(
        "cow_storage_daemon.app.validate_reflink_support",
        new_callable=AsyncMock,
        return_value=None,
    ):
        app = await create_app(
            {
                "base_path": str(tmp_path),
                "api_key": "test-key",
                "db_path": str(tmp_path / ".cow-daemon.db"),
            }
        )
    return app


class TestRouteDestPathWiring:
    """Tests verifying dest_path is threaded from route handler to submit_clone_job (Codex B3)."""

    async def test_post_clones_with_dest_path_calls_submit_with_dest_path(self, tmp_path):
        """POST /api/v1/clones with dest_path body field passes it to submit_clone_job."""
        app = await _make_test_app(tmp_path)

        dest = str(tmp_path / ".versioned" / "alias" / "v_1")
        captured_kwargs = {}

        async def mock_submit(**kwargs):
            captured_kwargs.update(kwargs)
            return "mock-job-id"

        app.state.clone_manager.submit_clone_job = mock_submit

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/clones",
                json={
                    "source_path": str(tmp_path),
                    "namespace": "ns",
                    "name": "v_1",
                    "dest_path": dest,
                },
                headers={"Authorization": "Bearer test-key"},
            )

        assert resp.status_code == 202
        assert "dest_path" in captured_kwargs
        assert captured_kwargs["dest_path"] == dest

        await app.state.store.close()

    async def test_post_clones_without_dest_path_calls_submit_with_dest_path_none(self, tmp_path):
        """POST /api/v1/clones without dest_path passes dest_path=None to submit_clone_job."""
        app = await _make_test_app(tmp_path)

        captured_kwargs = {}

        async def mock_submit(**kwargs):
            captured_kwargs.update(kwargs)
            return "mock-job-id"

        app.state.clone_manager.submit_clone_job = mock_submit

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/clones",
                json={
                    "source_path": str(tmp_path),
                    "namespace": "ns",
                    "name": "v_2",
                },
                headers={"Authorization": "Bearer test-key"},
            )

        assert resp.status_code == 202
        assert "dest_path" in captured_kwargs
        assert captured_kwargs["dest_path"] is None

        await app.state.store.close()

    async def test_post_clones_with_invalid_dest_path_returns_400(self, tmp_path):
        """POST /api/v1/clones with dest_path outside storage returns HTTP 400."""
        from cow_storage_daemon.core.clone_manager import PathNotAllowedError
        app = await _make_test_app(tmp_path)

        async def mock_submit(**kwargs):
            raise PathNotAllowedError("dest_path must be under storage_path: /etc/foo")

        app.state.clone_manager.submit_clone_job = mock_submit

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/clones",
                json={
                    "source_path": str(tmp_path),
                    "namespace": "ns",
                    "name": "v_3",
                    "dest_path": "/etc/foo",
                },
                headers={"Authorization": "Bearer test-key"},
            )

        assert resp.status_code == 400
        body = resp.json()
        assert body.get("code") == "PATH_NOT_ALLOWED"

        await app.state.store.close()


class TestHealthVersion:
    """Tests verifying GET /api/v1/health returns version field (Codex O4)."""

    async def test_health_endpoint_includes_version(self, tmp_path):
        """Health endpoint must return version field populated from __version__."""
        from cow_storage_daemon import __version__
        app = await _make_test_app(tmp_path)

        # Mock health_service.get_health to return minimal valid data
        async def mock_get_health():
            return {
                "status": "healthy",
                "filesystem_type": "xfs",
                "cow_method": "reflink",
                "disk_total_bytes": 1000,
                "disk_used_bytes": 400,
                "disk_available_bytes": 600,
                "uptime_seconds": 1.0,
            }

        app.state.clone_manager  # ensure app initialized
        # Patch the health service on the router's closure
        with patch(
            "cow_storage_daemon.health.health_service.HealthService.get_health",
            side_effect=mock_get_health,
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/v1/health")

        assert resp.status_code == 200
        body = resp.json()
        assert "version" in body
        assert body["version"] == __version__

        await app.state.store.close()


class TestReflinkValidationAtStartup:
    """Tests verifying validate_reflink_support is called during app startup (FIX 1)."""

    async def test_create_app_calls_validate_reflink_support(self, tmp_path):
        """create_app must call validate_reflink_support with base_path at startup."""
        from cow_storage_daemon.app import create_app

        called_with = []

        async def mock_validate(path):
            called_with.append(path)

        with patch(
            "cow_storage_daemon.app.validate_reflink_support",
            side_effect=mock_validate,
        ):
            app = await create_app(
                {
                    "base_path": str(tmp_path),
                    "api_key": "test-key",
                    "db_path": str(tmp_path / ".cow-daemon.db"),
                }
            )
            await app.state.store.close()

        assert len(called_with) == 1, "validate_reflink_support must be called exactly once"
        assert called_with[0] == str(tmp_path)

    async def test_create_app_raises_when_reflink_not_supported(self, tmp_path):
        """create_app must raise ReflinkNotSupportedError when filesystem lacks reflink."""
        from cow_storage_daemon.app import create_app

        with patch(
            "cow_storage_daemon.app.validate_reflink_support",
            side_effect=ReflinkNotSupportedError("not supported"),
        ):
            with pytest.raises(ReflinkNotSupportedError):
                await create_app(
                    {
                        "base_path": str(tmp_path),
                        "api_key": "test-key",
                        "db_path": str(tmp_path / ".cow-daemon.db"),
                    }
                )

    async def test_create_app_succeeds_when_reflink_supported(self, tmp_path):
        """create_app completes normally when validate_reflink_support does not raise."""
        from cow_storage_daemon.app import create_app

        with patch(
            "cow_storage_daemon.app.validate_reflink_support",
            new_callable=AsyncMock,
            return_value=None,
        ):
            app = await create_app(
                {
                    "base_path": str(tmp_path),
                    "api_key": "test-key",
                    "db_path": str(tmp_path / ".cow-daemon.db"),
                }
            )
            assert app is not None
            await app.state.store.close()


class TestShutdownCleanup:
    """Tests verifying store.close() is called on shutdown (FIX 6)."""

    async def test_store_close_called_on_shutdown(self, tmp_path):
        """store.close() must be called during app shutdown lifespan.

        Uses starlette's TestClient which properly triggers ASGI lifespan
        startup/shutdown events (httpx's ASGITransport does not).
        """
        from cow_storage_daemon.app import create_app
        from cow_storage_daemon.core.metadata_store import MetadataStore

        closed = []
        original_close = MetadataStore.close

        async def tracking_close(self):
            closed.append(True)
            await original_close(self)

        with patch(
            "cow_storage_daemon.app.validate_reflink_support",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with patch.object(MetadataStore, "close", tracking_close):
                app = await create_app(
                    {
                        "base_path": str(tmp_path),
                        "api_key": "test-key",
                        "db_path": str(tmp_path / ".cow-daemon.db"),
                    }
                )

                # starlette TestClient properly triggers ASGI lifespan events
                from starlette.testclient import TestClient
                with TestClient(app) as client:
                    resp = client.get("/api/v1/health")

        # After lifespan exits (TestClient context manager exit triggers shutdown),
        # store.close() must have been called
        assert len(closed) >= 1, "store.close() must be called on shutdown"
