"""Entry point for running the CoW Storage Daemon via python -m cow_storage_daemon."""

from __future__ import annotations

import asyncio
import json
import os
import sys


def main() -> None:
    """Load config from COW_DAEMON_CONFIG env var and start the daemon."""
    config_path = os.environ.get("COW_DAEMON_CONFIG")
    if not config_path:
        print("ERROR: COW_DAEMON_CONFIG environment variable not set", file=sys.stderr)
        print("Set it to the path of your config.json file", file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(config_path):
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    port = config.get("port", 8081)
    host = config.get("host", "0.0.0.0")

    # Import here to avoid slow startup for --help etc.
    import uvicorn

    from cow_storage_daemon.app import create_app

    async def _create():
        return await create_app(config)

    app = asyncio.run(_create())

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
