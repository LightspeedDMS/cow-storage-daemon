"""Integration tests for all REST API endpoints (AC1, AC2, AC3, AC9, AC10)."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


class TestHealthEndpoint:
    """Tests for GET /api/v1/health (AC1, AC2, AC10)."""

    async def test_health_returns_200(self, async_client):
        """Health endpoint returns 200 OK."""
        resp = await async_client.get("/api/v1/health")
        assert resp.status_code == 200

    async def test_health_response_structure(self, async_client):
        """Health response includes all required fields (AC10)."""
        resp = await async_client.get("/api/v1/health")
        data = resp.json()
        assert "status" in data
        assert "filesystem_type" in data
        assert "cow_method" in data
        assert "disk_total_bytes" in data
        assert "disk_used_bytes" in data
        assert "disk_available_bytes" in data
        assert "uptime_seconds" in data

    async def test_health_status_is_healthy(self, async_client):
        """Health status field is 'healthy'."""
        resp = await async_client.get("/api/v1/health")
        assert resp.json()["status"] == "healthy"

    async def test_health_no_auth_required_by_default(self, async_client):
        """Health endpoint accessible without auth when health_requires_auth=False."""
        resp = await async_client.get("/api/v1/health")
        assert resp.status_code == 200

    async def test_health_requires_auth_when_configured(self, config_dict, tmp_path):
        """Health returns 401 when health_requires_auth=True and no token provided."""
        from cow_storage_daemon.app import create_app
        from httpx import ASGITransport, AsyncClient

        config = {**config_dict, "health_requires_auth": True}
        app = await create_app(config)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/api/v1/health")
            assert resp.status_code == 401


class TestAuthenticationRequired:
    """Tests for authentication enforcement (AC2)."""

    async def test_list_clones_requires_auth(self, async_client):
        """GET /api/v1/clones returns 401 without auth header."""
        resp = await async_client.get("/api/v1/clones")
        assert resp.status_code == 401

    async def test_create_clone_requires_auth(self, async_client):
        """POST /api/v1/clones returns 401 without auth header."""
        resp = await async_client.post("/api/v1/clones", json={})
        assert resp.status_code == 401

    async def test_get_clone_requires_auth(self, async_client):
        """GET /api/v1/clones/{ns}/{name} returns 401 without auth."""
        resp = await async_client.get("/api/v1/clones/ns/name")
        assert resp.status_code == 401

    async def test_delete_clone_requires_auth(self, async_client):
        """DELETE /api/v1/clones/{ns}/{name} returns 401 without auth."""
        resp = await async_client.delete("/api/v1/clones/ns/name")
        assert resp.status_code == 401

    async def test_get_job_requires_auth(self, async_client):
        """GET /api/v1/jobs/{id} returns 401 without auth."""
        resp = await async_client.get("/api/v1/jobs/some-job-id")
        assert resp.status_code == 401

    async def test_stats_requires_auth(self, async_client):
        """GET /api/v1/stats returns 401 without auth."""
        resp = await async_client.get("/api/v1/stats")
        assert resp.status_code == 401

    async def test_invalid_token_returns_401(self, async_client):
        """Invalid Bearer token returns 401."""
        resp = await async_client.get(
            "/api/v1/clones",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401

    async def test_error_response_has_error_and_code(self, async_client):
        """401 response body includes {error, code} (AC9)."""
        resp = await async_client.get("/api/v1/clones")
        data = resp.json()
        assert "error" in data
        assert "code" in data
        assert data["code"] == "UNAUTHORIZED"


class TestCreateCloneEndpoint:
    """Tests for POST /api/v1/clones (AC1, AC3, AC4)."""

    async def test_returns_202_accepted(self, async_client, auth_headers, tmp_path):
        """POST /api/v1/clones returns 202 Accepted."""
        source = tmp_path / "source"
        source.mkdir()
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            resp = await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
        assert resp.status_code == 202

    async def test_response_includes_job_id(self, async_client, auth_headers, tmp_path):
        """POST /api/v1/clones response includes job_id and status=pending."""
        source = tmp_path / "source"
        source.mkdir()
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            resp = await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
        data = resp.json()
        assert "job_id" in data
        assert "status" in data
        assert data["status"] == "pending"

    async def test_invalid_namespace_returns_400(self, async_client, auth_headers):
        """Invalid namespace format returns 400 Bad Request."""
        resp = await async_client.post(
            "/api/v1/clones",
            json={"source_path": "/data", "namespace": "bad/ns", "name": "c1"},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    async def test_missing_source_path_returns_400(self, async_client, auth_headers):
        """Missing source_path returns 400 Bad Request."""
        resp = await async_client.post(
            "/api/v1/clones",
            json={"namespace": "ns", "name": "c1"},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    async def test_duplicate_clone_returns_409(self, async_client, auth_headers, tmp_path):
        """Creating a clone with duplicate namespace+name returns 409 Conflict."""
        source = tmp_path / "source"
        source.mkdir()
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
            # Wait for first clone to complete
            await asyncio.sleep(0.2)
            resp = await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
        assert resp.status_code == 409
        data = resp.json()
        assert data["code"] == "CONFLICT"


class TestGetJobEndpoint:
    """Tests for GET /api/v1/jobs/{job_id} (AC1, AC3)."""

    async def test_poll_job_returns_200(self, async_client, auth_headers, tmp_path):
        """GET /api/v1/jobs/{id} returns 200 for existing job."""
        source = tmp_path / "source"
        source.mkdir()
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            create_resp = await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
            job_id = create_resp.json()["job_id"]
            resp = await async_client.get(f"/api/v1/jobs/{job_id}", headers=auth_headers)
        assert resp.status_code == 200

    async def test_poll_nonexistent_job_returns_404(self, async_client, auth_headers):
        """GET /api/v1/jobs/{id} returns 404 for unknown job."""
        resp = await async_client.get("/api/v1/jobs/nonexistent-id", headers=auth_headers)
        assert resp.status_code == 404
        data = resp.json()
        assert data["code"] == "NOT_FOUND"

    async def test_completed_job_includes_clone_path(self, async_client, auth_headers, tmp_path):
        """Completed job response includes clone_path."""
        source = tmp_path / "source"
        source.mkdir()
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            create_resp = await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
            job_id = create_resp.json()["job_id"]

            # Poll until completed
            for _ in range(20):
                resp = await async_client.get(f"/api/v1/jobs/{job_id}", headers=auth_headers)
                if resp.json()["status"] == "completed":
                    break
                await asyncio.sleep(0.05)

            data = resp.json()
            assert data["status"] == "completed"
            assert data["clone_path"] is not None


class TestGetCloneEndpoint:
    """Tests for GET /api/v1/clones/{namespace}/{name} (AC1)."""

    async def _create_and_wait(self, client, auth_headers, source_path, namespace, name):
        """Helper: create a clone and wait for completion."""
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            resp = await client.post(
                "/api/v1/clones",
                json={"source_path": source_path, "namespace": namespace, "name": name},
                headers=auth_headers,
            )
            job_id = resp.json()["job_id"]
            for _ in range(20):
                job_resp = await client.get(f"/api/v1/jobs/{job_id}", headers=auth_headers)
                if job_resp.json()["status"] == "completed":
                    break
                await asyncio.sleep(0.05)

    async def test_get_existing_clone_returns_200(self, async_client, auth_headers, tmp_path):
        """GET /api/v1/clones/{ns}/{name} returns 200 for existing clone."""
        source = tmp_path / "source"
        source.mkdir()
        await self._create_and_wait(async_client, auth_headers, str(source), "ns", "c1")
        resp = await async_client.get("/api/v1/clones/ns/c1", headers=auth_headers)
        assert resp.status_code == 200

    async def test_get_clone_response_includes_fields(self, async_client, auth_headers, tmp_path):
        """Clone info response includes path, size, created_at, source_path (AC1)."""
        source = tmp_path / "source"
        source.mkdir()
        await self._create_and_wait(async_client, auth_headers, str(source), "ns", "c1")
        resp = await async_client.get("/api/v1/clones/ns/c1", headers=auth_headers)
        data = resp.json()
        assert "clone_path" in data
        assert "source_path" in data
        assert "created_at" in data

    async def test_get_nonexistent_clone_returns_404(self, async_client, auth_headers):
        """GET /api/v1/clones/{ns}/{name} returns 404 for unknown clone."""
        resp = await async_client.get("/api/v1/clones/ns/nonexistent", headers=auth_headers)
        assert resp.status_code == 404
        data = resp.json()
        assert data["code"] == "NOT_FOUND"


class TestListClonesEndpoint:
    """Tests for GET /api/v1/clones (AC1, AC4)."""

    async def test_list_clones_returns_200(self, async_client, auth_headers):
        """GET /api/v1/clones returns 200."""
        resp = await async_client.get("/api/v1/clones", headers=auth_headers)
        assert resp.status_code == 200

    async def test_list_clones_returns_list(self, async_client, auth_headers):
        """GET /api/v1/clones returns a list."""
        resp = await async_client.get("/api/v1/clones", headers=auth_headers)
        assert isinstance(resp.json(), list)

    async def test_list_clones_namespace_filter(self, async_client, auth_headers):
        """GET /api/v1/clones?namespace=X filters by namespace."""
        resp = await async_client.get("/api/v1/clones?namespace=ns1", headers=auth_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


class TestDeleteCloneEndpoint:
    """Tests for DELETE /api/v1/clones/{namespace}/{name} (AC1, AC8)."""

    async def test_delete_existing_clone_returns_200(self, async_client, auth_headers, tmp_path):
        """DELETE /api/v1/clones/{ns}/{name} returns 200 for existing clone."""
        source = tmp_path / "source"
        source.mkdir()
        with patch(
            "cow_storage_daemon.core.filesystem.perform_reflink_copy",
            new_callable=AsyncMock,
            return_value=None,
        ):
            resp = await async_client.post(
                "/api/v1/clones",
                json={"source_path": str(source), "namespace": "ns", "name": "c1"},
                headers=auth_headers,
            )
            job_id = resp.json()["job_id"]
            for _ in range(20):
                job_resp = await async_client.get(f"/api/v1/jobs/{job_id}", headers=auth_headers)
                if job_resp.json()["status"] == "completed":
                    break
                await asyncio.sleep(0.05)

        resp = await async_client.delete("/api/v1/clones/ns/c1", headers=auth_headers)
        assert resp.status_code == 200

    async def test_delete_nonexistent_clone_returns_404(self, async_client, auth_headers):
        """DELETE /api/v1/clones/{ns}/{name} returns 404 for unknown clone."""
        resp = await async_client.delete("/api/v1/clones/ns/nonexistent", headers=auth_headers)
        assert resp.status_code == 404
        data = resp.json()
        assert data["code"] == "NOT_FOUND"


class TestPathNotAllowedEndpoint:
    """Tests for source_path validation through the HTTP layer (AC9, HIGH-1 fix)."""

    async def test_create_clone_disallowed_source_returns_400(self, config_dict, tmp_path):
        """POST /api/v1/clones with disallowed source_path returns 400 PATH_NOT_ALLOWED."""
        from cow_storage_daemon.app import create_app
        from httpx import ASGITransport, AsyncClient

        allowed_root = str(tmp_path / "allowed")
        (tmp_path / "allowed").mkdir()
        disallowed_source = str(tmp_path / "forbidden" / "data")
        (tmp_path / "forbidden" / "data").mkdir(parents=True)
        (tmp_path / "forbidden" / "data" / "file.txt").write_text("secret")

        config = {**config_dict, "allowed_source_roots": [allowed_root]}
        app = await create_app(config)
        headers = {"Authorization": f"Bearer {config['api_key']}"}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/clones",
                json={"source_path": disallowed_source, "namespace": "test", "name": "clone1"},
                headers=headers,
            )
            assert resp.status_code == 400
            data = resp.json()
            assert data["code"] == "PATH_NOT_ALLOWED"
        await app.state.store.close()

    async def test_create_clone_allowed_source_returns_202(self, config_dict, tmp_path):
        """POST /api/v1/clones with allowed source_path returns 202."""
        from cow_storage_daemon.app import create_app
        from httpx import ASGITransport, AsyncClient

        allowed_root = str(tmp_path / "allowed")
        (tmp_path / "allowed").mkdir()
        source = str(tmp_path / "allowed" / "repo")
        (tmp_path / "allowed" / "repo").mkdir()
        (tmp_path / "allowed" / "repo" / "file.txt").write_text("data")

        config = {**config_dict, "allowed_source_roots": [allowed_root]}
        app = await create_app(config)
        headers = {"Authorization": f"Bearer {config['api_key']}"}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/clones",
                json={"source_path": source, "namespace": "test", "name": "allowed1"},
                headers=headers,
            )
            assert resp.status_code == 202
        import asyncio
        await asyncio.sleep(0.1)
        await app.state.store.close()


class TestStatsEndpoint:
    """Tests for GET /api/v1/stats (AC1, AC10)."""

    async def test_stats_returns_200(self, async_client, auth_headers):
        """GET /api/v1/stats returns 200."""
        resp = await async_client.get("/api/v1/stats", headers=auth_headers)
        assert resp.status_code == 200

    async def test_stats_response_structure(self, async_client, auth_headers):
        """Stats response includes all required fields (AC10)."""
        resp = await async_client.get("/api/v1/stats", headers=auth_headers)
        data = resp.json()
        assert "disk_total_bytes" in data
        assert "disk_used_bytes" in data
        assert "disk_available_bytes" in data
        assert "clone_count_total" in data
        assert "clones_by_namespace" in data
