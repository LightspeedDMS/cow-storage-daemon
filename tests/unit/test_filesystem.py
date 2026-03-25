"""Tests for filesystem operations (AC5, AC8, AC10)."""

from unittest.mock import AsyncMock, patch

import pytest

from cow_storage_daemon.core.filesystem import (
    FilesystemError,
    ReflinkNotSupportedError,
    get_disk_stats,
    get_filesystem_type,
    perform_reflink_copy,
    validate_reflink_support,
)


class TestValidateReflinkSupport:
    """Tests for reflink validation at startup (AC5)."""

    async def test_raises_when_reflink_not_supported(self, tmp_path):
        """validate_reflink_support raises ReflinkNotSupportedError when _run_cp_reflink fails."""
        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            new_callable=AsyncMock,
            side_effect=ReflinkNotSupportedError("Filesystem does not support reflink"),
        ):
            with pytest.raises(ReflinkNotSupportedError):
                await validate_reflink_support(str(tmp_path))

    async def test_succeeds_when_reflink_supported(self, tmp_path):
        """validate_reflink_support completes without error when _run_cp_reflink succeeds."""
        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            new_callable=AsyncMock,
            return_value=None,
        ):
            await validate_reflink_support(str(tmp_path))

    async def test_cleans_up_test_files_on_success(self, tmp_path):
        """Test files created during validation are cleaned up on success."""
        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            new_callable=AsyncMock,
            return_value=None,
        ):
            await validate_reflink_support(str(tmp_path))
            test_files = list(tmp_path.glob(".reflink-test-*"))
            assert len(test_files) == 0

    async def test_cleans_up_test_files_on_failure(self, tmp_path):
        """Test files created during validation are cleaned up even on failure."""
        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            new_callable=AsyncMock,
            side_effect=ReflinkNotSupportedError("not supported"),
        ):
            with pytest.raises(ReflinkNotSupportedError):
                await validate_reflink_support(str(tmp_path))
            test_files = list(tmp_path.glob(".reflink-test-*"))
            assert len(test_files) == 0

    async def test_uses_reflink_always_flag(self, tmp_path):
        """validate_reflink_support must invoke subprocess with --reflink=always."""
        import subprocess
        from unittest.mock import MagicMock

        completed = MagicMock()
        completed.returncode = 0
        completed.stderr = ""

        with patch(
            "cow_storage_daemon.core.filesystem.subprocess.run",
            return_value=completed,
        ) as mock_run:
            await validate_reflink_support(str(tmp_path))

        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "--reflink=always" in cmd
        assert "--reflink=auto" not in cmd


class TestPerformReflinkCopy:
    """Tests for performing reflink copy operations (AC3, AC5)."""

    async def test_calls_cp_reflink_with_source_and_dest(self, tmp_path):
        """perform_reflink_copy calls _run_cp_reflink with correct source and dest."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_cp:
            await perform_reflink_copy(str(source), str(dest))
            mock_cp.assert_called_once()
            call_str = str(mock_cp.call_args)
            assert str(source) in call_str
            assert str(dest) in call_str

    async def test_raises_filesystem_error_on_cp_failure(self, tmp_path):
        """perform_reflink_copy raises FilesystemError when _run_cp_reflink fails."""
        source = tmp_path / "source"
        dest = tmp_path / "dest"
        source.mkdir()

        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            new_callable=AsyncMock,
            side_effect=FilesystemError("cp failed"),
        ):
            with pytest.raises(FilesystemError):
                await perform_reflink_copy(str(source), str(dest))

    async def test_raises_when_source_not_found(self, tmp_path):
        """perform_reflink_copy raises FilesystemError when source does not exist."""
        source = tmp_path / "nonexistent"
        dest = tmp_path / "dest"

        with pytest.raises(FilesystemError) as exc_info:
            await perform_reflink_copy(str(source), str(dest))
        error_msg = str(exc_info.value).lower()
        assert "source" in error_msg or "not found" in error_msg or "exist" in error_msg

    async def test_concurrent_copies_different_sources_run_in_parallel(self, tmp_path):
        """Concurrent copies from different sources should not block each other."""
        import asyncio

        source1 = tmp_path / "source1"
        source2 = tmp_path / "source2"
        source1.mkdir()
        source2.mkdir()
        dest1 = tmp_path / "dest1"
        dest2 = tmp_path / "dest2"

        async def slow_cp(*args, **kwargs):
            await asyncio.sleep(0.05)

        with patch(
            "cow_storage_daemon.core.filesystem._run_cp_reflink",
            side_effect=slow_cp,
        ):
            start = asyncio.get_event_loop().time()
            await asyncio.gather(
                perform_reflink_copy(str(source1), str(dest1)),
                perform_reflink_copy(str(source2), str(dest2)),
            )
            elapsed = asyncio.get_event_loop().time() - start
            assert elapsed < 0.09, f"Expected parallel execution, took {elapsed:.3f}s"


class TestGetDiskStats:
    """Tests for disk statistics retrieval (AC10)."""

    async def test_returns_disk_stats_dict(self, tmp_path):
        """get_disk_stats returns dict with total, used, available bytes."""
        stats = await get_disk_stats(str(tmp_path))
        assert "total_bytes" in stats
        assert "used_bytes" in stats
        assert "available_bytes" in stats

    async def test_stats_are_non_negative_integers(self, tmp_path):
        """Disk stats should be non-negative integers."""
        stats = await get_disk_stats(str(tmp_path))
        assert isinstance(stats["total_bytes"], int)
        assert isinstance(stats["used_bytes"], int)
        assert isinstance(stats["available_bytes"], int)
        assert stats["total_bytes"] >= 0
        assert stats["used_bytes"] >= 0
        assert stats["available_bytes"] >= 0

    async def test_total_approximately_equals_used_plus_available(self, tmp_path):
        """total_bytes should approximately equal used_bytes + available_bytes."""
        stats = await get_disk_stats(str(tmp_path))
        total = stats["total_bytes"]
        used_plus_avail = stats["used_bytes"] + stats["available_bytes"]
        # Allow 10% discrepancy for reserved blocks
        assert abs(total - used_plus_avail) / max(total, 1) < 0.1


class TestGetFilesystemType:
    """Tests for filesystem type detection (AC10)."""

    async def test_returns_non_empty_string(self, tmp_path):
        """get_filesystem_type returns a non-empty string."""
        fs_type = await get_filesystem_type(str(tmp_path))
        assert isinstance(fs_type, str)
        assert len(fs_type) > 0


class TestRunCpReflinkDirect:
    """Tests for _run_cp_reflink subprocess execution (lines 39-44)."""

    async def test_reflink_not_supported_error_on_reflink_stderr(self, tmp_path):
        """_run_cp_reflink raises ReflinkNotSupportedError when cp stderr mentions reflink."""
        from unittest.mock import MagicMock
        from cow_storage_daemon.core.filesystem import _run_cp_reflink

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "cp: failed to clone: Operation not supported"

        with patch("cow_storage_daemon.core.filesystem.subprocess.run", return_value=mock_result):
            with pytest.raises(ReflinkNotSupportedError):
                await _run_cp_reflink("/src", "/dst")

    async def test_filesystem_error_on_generic_cp_failure(self, tmp_path):
        """_run_cp_reflink raises FilesystemError on non-reflink cp failure."""
        from unittest.mock import MagicMock
        from cow_storage_daemon.core.filesystem import _run_cp_reflink

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "cp: cannot stat '/src': No such file or directory"

        with patch("cow_storage_daemon.core.filesystem.subprocess.run", return_value=mock_result):
            with pytest.raises(FilesystemError):
                await _run_cp_reflink("/src", "/dst")

    async def test_success_on_zero_exit_code(self, tmp_path):
        """_run_cp_reflink succeeds when cp returns exit code 0."""
        from unittest.mock import MagicMock
        from cow_storage_daemon.core.filesystem import _run_cp_reflink

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""

        with patch("cow_storage_daemon.core.filesystem.subprocess.run", return_value=mock_result):
            await _run_cp_reflink("/src", "/dst")  # Should not raise


class TestGetFilesystemTypeFallback:
    """Tests for /proc/mounts fallback and exception handling in get_filesystem_type."""

    async def test_falls_back_to_proc_mounts_when_df_fails(self, tmp_path):
        """get_filesystem_type falls back to /proc/mounts when df returns non-zero."""
        from unittest.mock import MagicMock

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("cow_storage_daemon.core.filesystem.subprocess.run", return_value=mock_result):
            fs_type = await get_filesystem_type(str(tmp_path))
            assert isinstance(fs_type, str)
            assert len(fs_type) > 0

    async def test_handles_df_timeout_gracefully(self, tmp_path):
        """get_filesystem_type handles subprocess.TimeoutExpired by falling back."""
        import subprocess as sp

        with patch(
            "cow_storage_daemon.core.filesystem.subprocess.run",
            side_effect=sp.TimeoutExpired(cmd=["df"], timeout=5),
        ):
            fs_type = await get_filesystem_type(str(tmp_path))
            assert isinstance(fs_type, str)

    async def test_handles_df_not_found_gracefully(self, tmp_path):
        """get_filesystem_type handles FileNotFoundError (df not installed) by falling back."""
        with patch(
            "cow_storage_daemon.core.filesystem.subprocess.run",
            side_effect=FileNotFoundError("df not found"),
        ):
            fs_type = await get_filesystem_type(str(tmp_path))
            assert isinstance(fs_type, str)

    async def test_returns_unknown_when_proc_mounts_unreadable(self, tmp_path):
        """get_filesystem_type returns 'unknown' when /proc/mounts cannot be read."""
        from unittest.mock import MagicMock, mock_open

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("cow_storage_daemon.core.filesystem.subprocess.run", return_value=mock_result):
            with patch("builtins.open", side_effect=OSError("cannot read /proc/mounts")):
                fs_type = await get_filesystem_type(str(tmp_path))
                assert fs_type == "unknown"


class TestCustomExceptions:
    """Tests for custom exception types."""

    def test_reflink_not_supported_error_is_exception(self):
        """ReflinkNotSupportedError should be an Exception subclass."""
        err = ReflinkNotSupportedError("test")
        assert isinstance(err, Exception)
        assert str(err) == "test"

    def test_filesystem_error_is_exception(self):
        """FilesystemError should be an Exception subclass."""
        err = FilesystemError("test")
        assert isinstance(err, Exception)
        assert str(err) == "test"
