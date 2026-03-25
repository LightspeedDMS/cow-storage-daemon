"""Shared test fixtures for cow-storage-daemon tests."""

import tempfile
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_api_key() -> str:
    """Provide a test API key."""
    return "test-api-key-1234567890abcdef"


@pytest.fixture
def auth_headers(test_api_key: str) -> dict:
    """Provide auth headers with Bearer token."""
    return {"Authorization": f"Bearer {test_api_key}"}


@pytest.fixture
def config_dict(temp_dir: Path, test_api_key: str) -> dict:
    """Provide a minimal config dictionary for testing."""
    return {
        "base_path": str(temp_dir),
        "api_key": test_api_key,
        "db_path": str(temp_dir / ".cow-daemon.db"),
        "health_requires_auth": False,
    }


@pytest_asyncio.fixture
async def async_client(config_dict: dict) -> AsyncGenerator[AsyncClient, None]:
    """Provide an async test client for the FastAPI app."""
    import asyncio
    from cow_storage_daemon.app import create_app

    app = await create_app(config_dict)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
    # Allow background tasks to complete before closing the store
    await asyncio.sleep(0.1)
    await app.state.store.close()
