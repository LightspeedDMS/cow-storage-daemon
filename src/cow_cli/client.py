"""CowClient — HTTP client for CoW Storage Daemon API.

Wraps httpx.Client with automatic token injection and error handling.
All requests use verify=False (no TLS cert verification, internal usage).
"""
import time
from typing import Any, List, Optional

import httpx


class CLIError(Exception):
    """User-facing CLI error with message."""


class CowClient:
    """HTTP client for CoW Storage Daemon REST API."""

    def __init__(self, base_url: str, token: str, timeout: float = 30.0) -> None:
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            verify=False,
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> Any:
        """Make HTTP request with error handling."""
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.ConnectError:
            raise CLIError(
                f"Could not connect to daemon at {self._client.base_url}. "
                "Verify the daemon is running."
            )
        except httpx.TimeoutException:
            raise CLIError(
                f"Request timed out after {self._client.timeout.read}s."
            )

        if response.status_code == 401:
            raise CLIError(
                "Authentication failed. Check your API token with "
                "'cow-cli update <alias> --token <new-token>'."
            )
        if response.status_code == 404:
            try:
                data = response.json()
                msg = data.get("error", "Not found.")
            except Exception:
                msg = "Not found."
            raise CLIError(msg)
        if response.status_code == 409:
            try:
                data = response.json()
                msg = data.get("error", "Conflict.")
            except Exception:
                msg = "Conflict."
            raise CLIError(msg)
        if response.status_code == 400:
            try:
                data = response.json()
                msg = data.get("error", "Bad request.")
            except Exception:
                msg = "Bad request."
            raise CLIError(msg)
        if response.status_code >= 400:
            try:
                data = response.json()
                msg = data.get("error", f"HTTP {response.status_code}")
            except Exception:
                msg = f"HTTP {response.status_code}"
            raise CLIError(msg)

        return response.json() if response.content else {}

    # --- Clone Operations ---

    def create_clone(self, source_path: str, namespace: str, name: str) -> dict:
        """POST /api/v1/clones — returns job info (202)."""
        return self._request(
            "POST", "/api/v1/clones",
            json={"source_path": source_path, "namespace": namespace, "name": name},
        )

    def get_job(self, job_id: str) -> dict:
        """GET /api/v1/jobs/{job_id} — returns job status."""
        return self._request("GET", f"/api/v1/jobs/{job_id}")

    def list_clones(self, namespace: Optional[str] = None) -> List[dict]:
        """GET /api/v1/clones — returns list of clones."""
        params = {}
        if namespace:
            params["namespace"] = namespace
        return self._request("GET", "/api/v1/clones", params=params)

    def get_clone(self, namespace: str, name: str) -> dict:
        """GET /api/v1/clones/{namespace}/{name} — returns clone info."""
        return self._request("GET", f"/api/v1/clones/{namespace}/{name}")

    def delete_clone(self, namespace: str, name: str) -> dict:
        """DELETE /api/v1/clones/{namespace}/{name} — returns deletion status."""
        return self._request("DELETE", f"/api/v1/clones/{namespace}/{name}")

    # --- Health/Stats (for Story #4, but define here for completeness) ---

    def health(self) -> dict:
        """GET /api/v1/health — returns health info."""
        return self._request("GET", "/api/v1/health")

    def stats(self) -> dict:
        """GET /api/v1/stats — returns storage statistics."""
        return self._request("GET", "/api/v1/stats")

    # --- Wait mode polling ---

    def wait_for_job(
        self, job_id: str, poll_interval: float = 2.0, timeout: float = 300.0,
        spinner_callback=None,
    ) -> dict:
        """Poll job until completed/failed or timeout.

        Args:
            job_id: Job UUID to poll.
            poll_interval: Seconds between polls.
            timeout: Maximum seconds to wait.
            spinner_callback: Optional callable(elapsed_seconds) for UI updates.

        Returns:
            Final job status dict.

        Raises:
            CLIError: On timeout or job failure.
        """
        start = time.monotonic()
        while True:
            job = self.get_job(job_id)
            status = job.get("status")

            if status == "completed":
                return job
            if status == "failed":
                error = job.get("error", "Unknown error")
                raise CLIError(f"Clone job failed: {error}")

            elapsed = time.monotonic() - start
            if elapsed >= timeout:
                raise CLIError(
                    f"Clone job timed out after {int(timeout)} seconds. "
                    f"Check status with: cow-cli job {job_id}"
                )

            if spinner_callback:
                spinner_callback(elapsed)

            time.sleep(poll_interval)
