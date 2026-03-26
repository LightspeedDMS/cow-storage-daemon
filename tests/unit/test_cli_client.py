"""Tests for CowClient HTTP wrapper."""
import pytest
import httpx

from cow_cli.client import CLIError, CowClient


def _make_client(handler, base_url="http://test:8081", token="test-token"):
    """Create a CowClient with a custom httpx transport for testing."""
    transport = httpx.MockTransport(handler)
    client = CowClient.__new__(CowClient)
    client._client = httpx.Client(
        base_url=base_url,
        headers={"Authorization": f"Bearer {token}"},
        transport=transport,
        verify=False,
    )
    return client


class TestCreateClone:
    def test_sends_correct_request(self):
        def handler(request):
            assert request.method == "POST"
            assert "/api/v1/clones" in str(request.url)
            import json
            body = json.loads(request.content)
            assert body["source_path"] == "/data/src"
            assert body["namespace"] == "test"
            assert body["name"] == "clone1"
            return httpx.Response(202, json={"job_id": "abc-123", "status": "pending"})

        client = _make_client(handler)
        result = client.create_clone("/data/src", "test", "clone1")
        assert result["job_id"] == "abc-123"

    def test_auth_header_included(self):
        def handler(request):
            assert request.headers["authorization"] == "Bearer test-token"
            return httpx.Response(202, json={"job_id": "x", "status": "pending"})

        client = _make_client(handler)
        client.create_clone("/src", "ns", "name")


class TestGetJob:
    def test_returns_job_status(self):
        def handler(request):
            assert "/api/v1/jobs/abc-123" in str(request.url)
            return httpx.Response(200, json={
                "job_id": "abc-123", "status": "completed",
                "namespace": "test", "name": "clone1",
                "source_path": "/data/src", "clone_path": "test/clone1",
            })

        client = _make_client(handler)
        result = client.get_job("abc-123")
        assert result["status"] == "completed"

    def test_404_raises_cli_error(self):
        def handler(request):
            return httpx.Response(404, json={"error": "Job not found", "code": "NOT_FOUND"})

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Job not found"):
            client.get_job("nonexistent")


class TestListClones:
    def test_returns_clone_list(self):
        def handler(request):
            return httpx.Response(200, json=[
                {"namespace": "test", "name": "c1", "source_path": "/src",
                 "clone_path": "test/c1", "created_at": "2026-01-01T00:00:00Z", "size_bytes": 1024},
            ])

        client = _make_client(handler)
        result = client.list_clones()
        assert len(result) == 1
        assert result[0]["name"] == "c1"

    def test_passes_namespace_filter(self):
        def handler(request):
            assert "namespace=prod" in str(request.url)
            return httpx.Response(200, json=[])

        client = _make_client(handler)
        client.list_clones(namespace="prod")


class TestGetClone:
    def test_returns_clone_info(self):
        def handler(request):
            assert "/api/v1/clones/test/clone1" in str(request.url)
            return httpx.Response(200, json={
                "namespace": "test", "name": "clone1", "source_path": "/src",
                "clone_path": "test/clone1", "created_at": "2026-01-01T00:00:00Z", "size_bytes": 2048,
            })

        client = _make_client(handler)
        result = client.get_clone("test", "clone1")
        assert result["clone_path"] == "test/clone1"

    def test_404_raises_cli_error(self):
        def handler(request):
            return httpx.Response(404, json={"error": "Clone not found", "code": "NOT_FOUND"})

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Clone not found"):
            client.get_clone("test", "nonexistent")


class TestDeleteClone:
    def test_returns_success(self):
        def handler(request):
            assert request.method == "DELETE"
            return httpx.Response(200, json={"status": "deleted", "namespace": "test", "name": "c1"})

        client = _make_client(handler)
        result = client.delete_clone("test", "c1")
        assert result["status"] == "deleted"


class TestErrorHandling:
    def test_401_raises_auth_error(self):
        def handler(request):
            return httpx.Response(401, json={"error": "Missing or invalid API key", "code": "UNAUTHORIZED"})

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Authentication failed"):
            client.list_clones()

    def test_409_raises_conflict(self):
        def handler(request):
            return httpx.Response(409, json={"error": "Clone already exists", "code": "CONFLICT"})

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Clone already exists"):
            client.create_clone("/src", "ns", "dup")

    def test_500_raises_generic_error(self):
        def handler(request):
            return httpx.Response(500, json={"error": "Internal server error"})

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Internal server error"):
            client.list_clones()


class TestWaitForJob:
    def test_returns_on_completed(self):
        call_count = 0
        def handler(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(200, json={"job_id": "j1", "status": "pending"})
            return httpx.Response(200, json={
                "job_id": "j1", "status": "completed",
                "namespace": "test", "name": "c1",
                "source_path": "/src", "clone_path": "test/c1",
            })

        client = _make_client(handler)
        result = client.wait_for_job("j1", poll_interval=0.01, timeout=5.0)
        assert result["status"] == "completed"
        assert call_count == 2

    def test_raises_on_failed(self):
        def handler(request):
            return httpx.Response(200, json={
                "job_id": "j1", "status": "failed", "error": "disk full",
                "namespace": "test", "name": "c1", "source_path": "/src",
            })

        client = _make_client(handler)
        with pytest.raises(CLIError, match="disk full"):
            client.wait_for_job("j1", poll_interval=0.01)

    def test_raises_on_timeout(self):
        def handler(request):
            return httpx.Response(200, json={
                "job_id": "j1", "status": "running",
                "namespace": "test", "name": "c1", "source_path": "/src",
            })

        client = _make_client(handler)
        with pytest.raises(CLIError, match="timed out"):
            client.wait_for_job("j1", poll_interval=0.01, timeout=0.05)


class TestNetworkErrors:
    def test_connect_error_raises_cli_error(self):
        def handler(request):
            raise httpx.ConnectError("Connection refused")

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Could not connect"):
            client.list_clones()

    def test_timeout_raises_cli_error(self):
        def handler(request):
            raise httpx.ReadTimeout("Read timed out")

        client = _make_client(handler)
        with pytest.raises(CLIError, match="timed out"):
            client.list_clones()

    def test_404_non_json_body(self):
        """404 from a reverse proxy might return HTML, not JSON."""
        def handler(request):
            return httpx.Response(404, text="<html>Not Found</html>")

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Not found"):
            client.get_clone("ns", "name")

    def test_409_non_json_body(self):
        def handler(request):
            return httpx.Response(409, text="<html>Conflict</html>")

        client = _make_client(handler)
        with pytest.raises(CLIError, match="Conflict"):
            client.create_clone("/src", "ns", "name")
