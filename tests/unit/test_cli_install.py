"""Tests for installation and entry point registration."""
import subprocess
import sys

import pytest

from cow_cli import __version__


def test_version_constant():
    """cow_cli.__version__ is a valid semver-like string."""
    assert isinstance(__version__, str)
    parts = __version__.split(".")
    assert len(parts) >= 2
    assert all(p.isdigit() for p in parts)


def test_module_entry_point():
    """python -m cow_cli --version works."""
    result = subprocess.run(
        [sys.executable, "-m", "cow_cli", "--version"],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0
    assert __version__ in result.stdout


def test_module_help_lists_all_commands():
    """python -m cow_cli --help lists all 12 commands."""
    result = subprocess.run(
        [sys.executable, "-m", "cow_cli", "--help"],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0
    expected_commands = [
        "activate", "clone", "connect", "connections", "delete",
        "disconnect", "health", "info", "job", "list", "stats", "update",
    ]
    for cmd in expected_commands:
        assert cmd in result.stdout, f"Command '{cmd}' not in --help output"


def test_cli_main_importable():
    """The CLI entry point function is importable."""
    from cow_cli.main import cli
    assert callable(cli)


def test_cow_cli_package_importable():
    """cow_cli package can be imported without errors."""
    import cow_cli
    assert hasattr(cow_cli, "__version__")


def test_cow_daemon_main_importable():
    """The daemon __main__ module is importable and exposes main()."""
    import cow_storage_daemon.__main__
    assert hasattr(cow_storage_daemon.__main__, "main")
    assert callable(cow_storage_daemon.__main__.main)
