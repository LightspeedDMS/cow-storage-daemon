"""Pydantic request/response models for CoW Storage Daemon API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, field_validator


_NAME_PATTERN_DESCRIPTION = "alphanumeric, hyphens, and underscores only"
_NAMESPACE_MAX_LEN = 64
_NAME_MAX_LEN = 128


def _validate_identifier(value: str, field_name: str, max_len: int) -> str:
    """Validate a namespace or clone name identifier."""
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    if len(value) > max_len:
        raise ValueError(f"{field_name} must not exceed {max_len} characters")
    for char in value:
        if not (char.isalnum() or char in ("-", "_")):
            raise ValueError(
                f"{field_name} must contain only {_NAME_PATTERN_DESCRIPTION}, got '{char}'"
            )
    return value


class CloneCreateRequest(BaseModel):
    """Request body for POST /api/v1/clones."""

    source_path: str
    namespace: str
    name: str

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, v: str) -> str:
        return _validate_identifier(v, "namespace", _NAMESPACE_MAX_LEN)

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _validate_identifier(v, "name", _NAME_MAX_LEN)


class JobStatus(str, Enum):
    """Possible states for an async clone job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class CloneJobResponse(BaseModel):
    """Response body for 202 Accepted on POST /api/v1/clones."""

    job_id: str
    status: JobStatus


class JobStatusResponse(BaseModel):
    """Response body for GET /api/v1/jobs/{job_id}."""

    job_id: str
    status: JobStatus
    namespace: str
    name: str
    source_path: str
    clone_path: Optional[str] = None
    error: Optional[str] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class CloneInfo(BaseModel):
    """Response body for GET /api/v1/clones/{namespace}/{name}."""

    namespace: str
    name: str
    source_path: str
    clone_path: str
    created_at: datetime
    size_bytes: int


class HealthResponse(BaseModel):
    """Response body for GET /api/v1/health."""

    status: str
    filesystem_type: str
    cow_method: str
    disk_total_bytes: int
    disk_used_bytes: int
    disk_available_bytes: int
    uptime_seconds: float


class StatsResponse(BaseModel):
    """Response body for GET /api/v1/stats."""

    disk_total_bytes: int
    disk_used_bytes: int
    disk_available_bytes: int
    clone_count_total: int
    clones_by_namespace: dict[str, int]


class ErrorResponse(BaseModel):
    """Standard error response body (AC9)."""

    error: str
    code: str
