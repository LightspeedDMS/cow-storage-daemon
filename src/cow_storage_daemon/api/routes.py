"""REST API route definitions for CoW Storage Daemon (AC1)."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse

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
from cow_storage_daemon.core.clone_manager import CloneManager, ConflictError, PathNotAllowedError


def _not_found(resource: str) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"error": f"{resource} not found", "code": "NOT_FOUND"},
    )


def create_router(
    clone_manager: CloneManager,
    health_service,
    require_auth,
    health_auth,
) -> APIRouter:
    """Build and return the API router with all routes configured."""
    router = APIRouter(prefix="/api/v1")

    @router.get("/health", response_model=HealthResponse)
    async def health(_auth: bool = Depends(health_auth)):
        data = await health_service.get_health()
        return HealthResponse(**data)

    @router.get("/stats", response_model=StatsResponse)
    async def stats(_auth: bool = Depends(require_auth)):
        data = await health_service.get_stats()
        return StatsResponse(**data)

    @router.post("/clones", status_code=status.HTTP_202_ACCEPTED, response_model=CloneJobResponse)
    async def create_clone(
        body: CloneCreateRequest,
        _auth: bool = Depends(require_auth),
    ):
        try:
            job_id = await clone_manager.submit_clone_job(
                source_path=body.source_path,
                namespace=body.namespace,
                name=body.name,
            )
        except PathNotAllowedError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": str(exc), "code": "PATH_NOT_ALLOWED"},
            )
        except ConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail={"error": str(exc), "code": "CONFLICT"},
            )
        return CloneJobResponse(job_id=job_id, status=JobStatus.PENDING)

    @router.get("/jobs/{job_id}", response_model=JobStatusResponse)
    async def get_job(job_id: str, _auth: bool = Depends(require_auth)):
        job = await clone_manager.get_job(job_id)
        if job is None:
            raise _not_found("Job")
        return JobStatusResponse(
            job_id=job["job_id"],
            status=JobStatus(job["status"]),
            namespace=job["namespace"],
            name=job["name"],
            source_path=job["source_path"],
            clone_path=job.get("clone_path"),
            error=job.get("error"),
            created_at=job.get("created_at"),
            completed_at=job.get("completed_at"),
        )

    @router.get("/clones", response_model=List[CloneInfo])
    async def list_clones(
        namespace: Optional[str] = None,
        _auth: bool = Depends(require_auth),
    ):
        clones = await clone_manager.list_clones(namespace=namespace)
        return [
            CloneInfo(
                namespace=c["namespace"],
                name=c["name"],
                source_path=c["source_path"],
                clone_path=c["clone_path"],
                created_at=c["created_at"],
                size_bytes=c["size_bytes"],
            )
            for c in clones
        ]

    @router.get("/clones/{namespace}/{name}", response_model=CloneInfo)
    async def get_clone(
        namespace: str,
        name: str,
        _auth: bool = Depends(require_auth),
    ):
        clone = await clone_manager.get_clone(namespace, name)
        if clone is None:
            raise _not_found("Clone")
        return CloneInfo(
            namespace=clone["namespace"],
            name=clone["name"],
            source_path=clone["source_path"],
            clone_path=clone["clone_path"],
            created_at=clone["created_at"],
            size_bytes=clone["size_bytes"],
        )

    @router.delete("/clones/{namespace}/{name}")
    async def delete_clone(
        namespace: str,
        name: str,
        _auth: bool = Depends(require_auth),
    ):
        deleted = await clone_manager.delete_clone(namespace, name)
        if not deleted:
            raise _not_found("Clone")
        return {"status": "deleted", "namespace": namespace, "name": name}

    return router
