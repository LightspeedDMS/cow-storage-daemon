"""Filesystem operations for CoW Storage Daemon (AC5, AC8, AC10).

All blocking I/O is dispatched via asyncio.to_thread() to avoid blocking the event loop.
Uses cp --reflink=always exclusively; no fallback to --reflink=auto (hard-fail design).
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import uuid
from pathlib import Path
from typing import Dict


class ReflinkNotSupportedError(Exception):
    """Raised when the filesystem does not support cp --reflink=always."""


class FilesystemError(Exception):
    """Raised for general filesystem operation failures."""


async def _run_cp_reflink(source: str, dest: str) -> None:
    """Execute cp --reflink=always -a SOURCE DEST in a thread.

    Raises ReflinkNotSupportedError if reflink is not supported.
    Raises FilesystemError for other cp failures.
    """

    def _blocking_cp() -> None:
        result = subprocess.run(
            ["cp", "--reflink=always", "-a", source, dest],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            stderr = result.stderr.lower()
            if "reflink" in stderr or "not supported" in stderr or "operation not supported" in stderr:
                raise ReflinkNotSupportedError(
                    f"Filesystem does not support --reflink=always: {result.stderr.strip()}"
                )
            raise FilesystemError(
                f"cp failed (exit {result.returncode}): {result.stderr.strip()}"
            )

    await asyncio.to_thread(_blocking_cp)


async def validate_reflink_support(base_path: str) -> None:
    """Verify the filesystem at base_path supports cp --reflink=always.

    Creates temporary test files, attempts a reflink clone, and cleans up.
    Raises ReflinkNotSupportedError if reflink is not supported (hard-fail, AC5).
    """
    test_id = uuid.uuid4().hex[:8]
    src_file = Path(base_path) / f".reflink-test-src-{test_id}"
    dst_file = Path(base_path) / f".reflink-test-dst-{test_id}"

    try:
        src_file.write_text("reflink-test")
        await _run_cp_reflink(str(src_file), str(dst_file))
    finally:
        for f in [src_file, dst_file]:
            if f.exists():
                f.unlink()


async def perform_reflink_copy(source: str, dest: str) -> None:
    """Perform a reflink copy from source to dest using cp --reflink=always -a.

    Raises FilesystemError if source does not exist or the copy fails.
    """
    if not Path(source).exists():
        raise FilesystemError(f"Source path does not exist: {source}")
    await _run_cp_reflink(source, dest)


async def get_disk_stats(path: str) -> Dict[str, int]:
    """Return disk usage statistics for the filesystem containing path.

    Returns dict with keys: total_bytes, used_bytes, available_bytes.
    """

    def _blocking_statvfs() -> Dict[str, int]:
        stat = os.statvfs(path)
        total = stat.f_frsize * stat.f_blocks
        available = stat.f_frsize * stat.f_bavail
        used = total - (stat.f_frsize * stat.f_bfree)
        return {
            "total_bytes": total,
            "used_bytes": used,
            "available_bytes": available,
        }

    return await asyncio.to_thread(_blocking_statvfs)


async def get_filesystem_type(path: str) -> str:
    """Detect the filesystem type for the given path.

    Returns a lowercase filesystem type string (e.g. 'xfs', 'btrfs', 'ext4').
    """

    def _blocking_detect() -> str:
        try:
            result = subprocess.run(
                ["df", "--output=fstype", path],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                lines = result.stdout.strip().splitlines()
                if len(lines) >= 2:
                    return lines[1].strip().lower()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        # Fallback: read /proc/mounts to determine filesystem type
        try:
            resolved = str(Path(path).resolve())
            with open("/proc/mounts") as f:
                best_match = ("", "unknown")
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3:
                        mount_point = parts[1]
                        fs_type = parts[2]
                        if resolved.startswith(mount_point) and len(mount_point) > len(best_match[0]):
                            best_match = (mount_point, fs_type)
            return best_match[1].lower()
        except OSError:
            return "unknown"

    return await asyncio.to_thread(_blocking_detect)
