"""Tests for app startup validation and lifespan management (FIX 1, FIX 6)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from cow_storage_daemon.core.filesystem import ReflinkNotSupportedError


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
