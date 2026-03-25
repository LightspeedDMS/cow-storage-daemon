"""Tests for API request/response models (AC1, AC3, AC4, AC9, AC10)."""

import pytest
from pydantic import ValidationError

from cow_storage_daemon.api.models import (
    CloneCreateRequest,
    CloneInfo,
    CloneJobResponse,
    ErrorResponse,
    HealthResponse,
    JobStatus,
    JobStatusResponse,
    StatsResponse,
)


class TestCloneCreateRequest:
    """Tests for clone creation request model."""

    def test_valid_request(self):
        req = CloneCreateRequest(
            source_path="/data/repos/myrepo",
            namespace="cidx",
            name="clone-001",
        )
        assert req.source_path == "/data/repos/myrepo"
        assert req.namespace == "cidx"
        assert req.name == "clone-001"

    def test_namespace_alphanumeric_hyphens_underscores(self):
        req = CloneCreateRequest(
            source_path="/data",
            namespace="my-namespace_01",
            name="my-clone_01",
        )
        assert req.namespace == "my-namespace_01"

    def test_namespace_rejects_spaces(self):
        with pytest.raises(ValidationError) as exc_info:
            CloneCreateRequest(
                source_path="/data",
                namespace="bad namespace",
                name="clone",
            )
        assert "namespace" in str(exc_info.value).lower() or "string" in str(exc_info.value).lower()

    def test_namespace_rejects_special_chars(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(
                source_path="/data",
                namespace="bad/namespace",
                name="clone",
            )

    def test_namespace_max_64_chars(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(
                source_path="/data",
                namespace="a" * 65,
                name="clone",
            )

    def test_namespace_exactly_64_chars_ok(self):
        req = CloneCreateRequest(
            source_path="/data",
            namespace="a" * 64,
            name="clone",
        )
        assert len(req.namespace) == 64

    def test_name_max_128_chars(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(
                source_path="/data",
                namespace="ns",
                name="a" * 129,
            )

    def test_name_exactly_128_chars_ok(self):
        req = CloneCreateRequest(
            source_path="/data",
            namespace="ns",
            name="a" * 128,
        )
        assert len(req.name) == 128

    def test_name_rejects_slashes(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(
                source_path="/data",
                namespace="ns",
                name="bad/name",
            )

    def test_namespace_rejects_empty_string(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(source_path="/data", namespace="", name="clone")

    def test_name_rejects_empty_string(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(source_path="/data", namespace="ns", name="")

    def test_source_path_required(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(namespace="ns", name="clone")

    def test_namespace_required(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(source_path="/data", name="clone")

    def test_name_required(self):
        with pytest.raises(ValidationError):
            CloneCreateRequest(source_path="/data", namespace="ns")


class TestJobStatus:
    """Tests for job status enum."""

    def test_all_states_defined(self):
        assert JobStatus.PENDING == "pending"
        assert JobStatus.RUNNING == "running"
        assert JobStatus.COMPLETED == "completed"
        assert JobStatus.FAILED == "failed"


class TestCloneJobResponse:
    """Tests for clone job response (202 Accepted body)."""

    def test_valid_response(self):
        resp = CloneJobResponse(job_id="job-123", status=JobStatus.PENDING)
        assert resp.job_id == "job-123"
        assert resp.status == JobStatus.PENDING

    def test_serializes_to_dict(self):
        resp = CloneJobResponse(job_id="job-abc", status=JobStatus.PENDING)
        data = resp.model_dump()
        assert data["job_id"] == "job-abc"
        assert data["status"] == "pending"


class TestJobStatusResponse:
    """Tests for job status poll response."""

    def test_pending_job(self):
        resp = JobStatusResponse(
            job_id="job-1",
            status=JobStatus.PENDING,
            namespace="ns",
            name="clone",
            source_path="/data",
        )
        assert resp.clone_path is None
        assert resp.error is None

    def test_completed_job_has_clone_path(self):
        resp = JobStatusResponse(
            job_id="job-1",
            status=JobStatus.COMPLETED,
            namespace="ns",
            name="clone",
            source_path="/data",
            clone_path="ns/clone",
        )
        assert resp.clone_path == "ns/clone"
        assert resp.error is None

    def test_failed_job_has_error(self):
        resp = JobStatusResponse(
            job_id="job-1",
            status=JobStatus.FAILED,
            namespace="ns",
            name="clone",
            source_path="/data",
            error="Source not found",
        )
        assert resp.error == "Source not found"
        assert resp.clone_path is None


class TestCloneInfo:
    """Tests for clone info response model."""

    def test_valid_clone_info(self):
        from datetime import datetime
        info = CloneInfo(
            namespace="ns",
            name="clone",
            source_path="/data/repo",
            clone_path="ns/clone",
            created_at=datetime(2024, 1, 1, 12, 0, 0),
            size_bytes=1024,
        )
        assert info.namespace == "ns"
        assert info.size_bytes == 1024


class TestHealthResponse:
    """Tests for health response model (AC10)."""

    def test_valid_health_response(self):
        resp = HealthResponse(
            status="healthy",
            filesystem_type="xfs",
            cow_method="reflink",
            disk_total_bytes=100_000_000_000,
            disk_used_bytes=50_000_000_000,
            disk_available_bytes=50_000_000_000,
            uptime_seconds=3600,
        )
        assert resp.status == "healthy"
        assert resp.cow_method == "reflink"

    def test_serializes_correctly(self):
        resp = HealthResponse(
            status="healthy",
            filesystem_type="xfs",
            cow_method="reflink",
            disk_total_bytes=1000,
            disk_used_bytes=400,
            disk_available_bytes=600,
            uptime_seconds=100,
        )
        data = resp.model_dump()
        assert "filesystem_type" in data
        assert "uptime_seconds" in data


class TestStatsResponse:
    """Tests for stats response model (AC10)."""

    def test_valid_stats_response(self):
        resp = StatsResponse(
            disk_total_bytes=1000,
            disk_used_bytes=400,
            disk_available_bytes=600,
            clone_count_total=5,
            clones_by_namespace={"cidx": 3, "claude": 2},
        )
        assert resp.clone_count_total == 5
        assert resp.clones_by_namespace["cidx"] == 3

    def test_empty_namespace_map(self):
        resp = StatsResponse(
            disk_total_bytes=1000,
            disk_used_bytes=0,
            disk_available_bytes=1000,
            clone_count_total=0,
            clones_by_namespace={},
        )
        assert resp.clones_by_namespace == {}


class TestErrorResponse:
    """Tests for error response model (AC9)."""

    def test_valid_error_response(self):
        err = ErrorResponse(error="Clone not found", code="NOT_FOUND")
        assert err.error == "Clone not found"
        assert err.code == "NOT_FOUND"

    def test_serializes_correctly(self):
        err = ErrorResponse(error="Already exists", code="CONFLICT")
        data = err.model_dump()
        assert data["error"] == "Already exists"
        assert data["code"] == "CONFLICT"
