"""FastAPI application factory for CoW Storage Daemon."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, status
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.responses import JSONResponse

from cow_storage_daemon.api.auth import make_health_verifier, make_verify_api_key
from cow_storage_daemon.api.routes import create_router
from cow_storage_daemon.core.clone_manager import CloneManager
from cow_storage_daemon.core.filesystem import validate_reflink_support
from cow_storage_daemon.core.metadata_store import MetadataStore
from cow_storage_daemon.health.health_service import HealthService

logger = logging.getLogger(__name__)


async def create_app(config: Dict[str, Any]) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        config: Dictionary with keys: base_path, api_key, db_path (optional),
                health_requires_auth (optional, default False),
                allowed_source_roots (optional, default []).

    Returns:
        A fully configured FastAPI application instance.

    Raises:
        ReflinkNotSupportedError: If the filesystem at base_path does not support
            cp --reflink=always (AC5, FIX 1).
    """
    base_path = config["base_path"]
    api_key = config["api_key"]
    db_path = config.get("db_path") or str(Path(base_path) / ".cow-daemon.db")
    health_requires_auth = config.get("health_requires_auth", False)
    allowed_source_roots = config.get("allowed_source_roots", [])

    Path(base_path).mkdir(parents=True, exist_ok=True)

    # FIX 1: Validate reflink support at startup - hard fail if not supported (AC5)
    await validate_reflink_support(base_path)

    if not allowed_source_roots:
        logger.warning(
            "allowed_source_roots is not configured - any source path will be accepted. "
            "Set allowed_source_roots to restrict which directories may be cloned."
        )

    store = MetadataStore(db_path)
    await store.initialize()

    clone_manager = CloneManager(
        base_path=base_path,
        store=store,
        allowed_source_roots=allowed_source_roots,
    )
    health_service = HealthService(base_path=base_path, store=store)

    require_auth = make_verify_api_key(api_key)
    health_auth = make_health_verifier(api_key, health_requires_auth=health_requires_auth)

    # FIX 6: Use lifespan context manager for proper shutdown cleanup
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        # Startup already done above (store initialized, reflink validated)
        yield
        # Shutdown: close store to checkpoint SQLite WAL
        await store.close()

    app = FastAPI(title="CoW Storage Daemon", version="0.1.0", lifespan=lifespan)

    # Custom HTTP exception handler: return detail directly, not wrapped in {"detail":...} (AC9)
    @app.exception_handler(HTTPException)
    async def http_exception_handler(
        request: Request, exc: HTTPException
    ) -> JSONResponse:
        content = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail), "code": "ERROR"}
        return JSONResponse(status_code=exc.status_code, content=content)

    # Custom 422 handler: return {error, code} format (AC9)
    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = exc.errors()
        message = "; ".join(
            f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in errors
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": message, "code": "VALIDATION_ERROR"},
        )

    router = create_router(
        clone_manager=clone_manager,
        health_service=health_service,
        require_auth=require_auth,
        health_auth=health_auth,
    )
    app.include_router(router)

    # Attach services to app state for test access and potential external cleanup
    app.state.store = store
    app.state.clone_manager = clone_manager

    return app
