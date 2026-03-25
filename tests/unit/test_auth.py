"""Tests for API key authentication (AC2)."""

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from cow_storage_daemon.api.auth import make_health_verifier, make_verify_api_key, verify_api_key


class TestVerifyApiKey:
    """Tests for the Bearer token verification dependency."""

    def test_valid_api_key_passes(self):
        """Valid API key should pass without raising."""
        verifier = make_verify_api_key("my-secret-key")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="my-secret-key")
        result = verifier(creds)
        assert result is True

    def test_invalid_api_key_raises_401(self):
        """Invalid API key should raise HTTP 401."""
        verifier = make_verify_api_key("my-secret-key")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="wrong-key")
        with pytest.raises(HTTPException) as exc_info:
            verifier(creds)
        assert exc_info.value.status_code == 401

    def test_missing_credentials_raises_401(self):
        """Missing credentials should raise HTTP 401."""
        verifier = make_verify_api_key("my-secret-key")
        with pytest.raises(HTTPException) as exc_info:
            verifier(None)
        assert exc_info.value.status_code == 401

    def test_error_response_includes_structured_detail(self):
        """401 error should include structured error detail with code=UNAUTHORIZED (AC9)."""
        verifier = make_verify_api_key("my-secret-key")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="bad-key")
        with pytest.raises(HTTPException) as exc_info:
            verifier(creds)
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert "error" in detail
        assert "code" in detail
        assert detail["code"] == "UNAUTHORIZED"

    def test_empty_api_key_raises_401(self):
        """Empty string API key should raise 401."""
        verifier = make_verify_api_key("my-secret-key")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="")
        with pytest.raises(HTTPException) as exc_info:
            verifier(creds)
        assert exc_info.value.status_code == 401

    def test_verify_api_key_and_make_verify_api_key_are_callable(self):
        """verify_api_key and make_verify_api_key should be importable and callable."""
        assert callable(verify_api_key)
        assert callable(make_verify_api_key)


class TestHealthAuthBypass:
    """Tests for health endpoint auth bypass (AC2)."""

    def test_health_no_auth_required_when_flag_false(self):
        """When health_requires_auth=False, health verifier allows None credentials."""
        verifier = make_health_verifier("my-secret-key", health_requires_auth=False)
        result = verifier(None)
        assert result is True

    def test_health_auth_required_when_flag_true(self):
        """When health_requires_auth=True, health verifier requires valid key."""
        verifier = make_health_verifier("my-secret-key", health_requires_auth=True)
        with pytest.raises(HTTPException) as exc_info:
            verifier(None)
        assert exc_info.value.status_code == 401

    def test_health_auth_required_valid_key_passes(self):
        """When health_requires_auth=True, valid key still passes."""
        verifier = make_health_verifier("my-secret-key", health_requires_auth=True)
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="my-secret-key")
        result = verifier(creds)
        assert result is True

    def test_health_no_auth_valid_key_still_passes(self):
        """When health_requires_auth=False, providing a valid key still passes."""
        verifier = make_health_verifier("my-secret-key", health_requires_auth=False)
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="my-secret-key")
        result = verifier(creds)
        assert result is True
