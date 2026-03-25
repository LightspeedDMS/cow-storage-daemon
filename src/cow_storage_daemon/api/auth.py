"""Bearer token authentication for CoW Storage Daemon API (AC2).

Uses hmac.compare_digest for constant-time comparison to prevent timing attacks.
"""

from __future__ import annotations

import hmac
from typing import Callable, Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer_scheme = HTTPBearer(auto_error=False)

_UNAUTHORIZED = HTTPException(
    status_code=401,
    detail={"error": "Missing or invalid API key", "code": "UNAUTHORIZED"},
)


def _check_key(credentials: Optional[HTTPAuthorizationCredentials], api_key: str) -> bool:
    """Verify credentials against the configured API key using constant-time comparison."""
    if credentials is None or not credentials.credentials:
        raise _UNAUTHORIZED
    # hmac.compare_digest requires bytes or equal-length strings; encoding to bytes is safest.
    if not hmac.compare_digest(credentials.credentials.encode(), api_key.encode()):
        raise _UNAUTHORIZED
    return True


def make_verify_api_key(api_key: str) -> Callable:
    """Return a FastAPI dependency that enforces Bearer token authentication."""

    def verify(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
    ) -> bool:
        return _check_key(credentials, api_key)

    return verify


def make_health_verifier(api_key: str, health_requires_auth: bool = False) -> Callable:
    """Return a FastAPI dependency for the health endpoint.

    When health_requires_auth is False, allows unauthenticated access.
    When True, enforces the same Bearer token check as other endpoints.
    """

    def verify(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
    ) -> bool:
        if not health_requires_auth:
            return True
        return _check_key(credentials, api_key)

    return verify


def verify_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> bool:
    """Placeholder verify function. Use make_verify_api_key() to create a configured verifier."""
    raise NotImplementedError(
        "Use make_verify_api_key(api_key) to create a configured verifier"
    )
