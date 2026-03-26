"""Health and stats monitoring commands for cow-cli."""
import sys

import click

from cow_cli.client import CLIError, CowClient
from cow_cli.config import ConnectionConfig, InvalidConfigError
from cow_cli.output import format_json, format_table


def _load_config(ctx) -> ConnectionConfig:
    """Load ConnectionConfig from context."""
    config_dir = ctx.obj.get('config_dir')
    cfg = ConnectionConfig(config_dir=config_dir)
    try:
        cfg.load()
    except InvalidConfigError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    return cfg


def _get_active_client(cfg) -> CowClient:
    """Build CowClient from active connection. Returns client (caller must close)."""
    if cfg.active is None or cfg.active not in cfg.connections:
        click.echo(
            "Error: No active connection. "
            "Run 'cow-cli connect <alias> <url> --token <token>' to register one.",
            err=True,
        )
        sys.exit(1)
    conn = cfg.connections[cfg.active]
    return CowClient(base_url=conn["url"], token=conn["token"])


def _human_size(size_bytes: int) -> str:
    """Convert bytes to human-readable size."""
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PB"


def _format_uptime(seconds: float) -> str:
    """Format seconds as human-readable uptime."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m {seconds % 60:.0f}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


@click.command("health")
@click.option("--all", "check_all", is_flag=True, default=False,
              help="Check health of all registered daemons")
@click.pass_context
def health_cmd(ctx, check_all):
    """Check daemon health status."""
    cfg = _load_config(ctx)
    json_mode = ctx.obj.get('json')

    if check_all:
        _health_all(cfg, json_mode)
    else:
        _health_active(cfg, json_mode)


def _health_active(cfg, json_mode):
    """Health check for active daemon only."""
    with _get_active_client(cfg) as client:
        try:
            data = client.health()
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    if json_mode:
        click.echo(format_json(data))
        return

    click.echo(f"Status:      {data.get('status', 'unknown')}")
    click.echo(f"Filesystem:  {data.get('filesystem_type', 'unknown')}")
    click.echo(f"CoW Method:  {data.get('cow_method', 'unknown')}")
    click.echo(f"Disk Total:  {_human_size(data.get('disk_total_bytes', 0))}")
    click.echo(f"Disk Used:   {_human_size(data.get('disk_used_bytes', 0))}")
    click.echo(f"Disk Free:   {_human_size(data.get('disk_available_bytes', 0))}")
    click.echo(f"Uptime:      {_format_uptime(data.get('uptime_seconds', 0))}")


def _health_all(cfg, json_mode):
    """Health check for all registered daemons."""
    conn_list = cfg.list_connections()
    if not conn_list:
        click.echo("No connections registered.")
        return

    results = []
    for conn in conn_list:
        alias = conn["alias"]
        url = conn["url"]
        token = conn["token"]
        active = conn["active"]

        try:
            with CowClient(base_url=url, token=token) as client:
                data = client.health()
            status = data.get("status", "unknown")
            uptime = data.get("uptime_seconds", 0)
            results.append({
                "alias": alias,
                "url": url,
                "active": active,
                "reachable": True,
                "status": status,
                "uptime_seconds": uptime,
            })
        except CLIError as exc:
            results.append({
                "alias": alias,
                "url": url,
                "active": active,
                "reachable": False,
                "status": str(exc),
                "uptime_seconds": 0,
            })

    if json_mode:
        click.echo(format_json(results))
        return

    headers = ["ALIAS", "URL", "ACTIVE", "STATUS", "UPTIME"]
    rows = []
    for r in results:
        if r["reachable"]:
            status_str = r["status"]
            uptime_str = _format_uptime(r["uptime_seconds"])
        else:
            status_str = "unreachable"
            uptime_str = "-"
        rows.append([
            r["alias"],
            r["url"],
            "*" if r["active"] else "",
            status_str,
            uptime_str,
        ])
    click.echo(format_table(headers, rows))


@click.command("stats")
@click.pass_context
def stats_cmd(ctx):
    """Show storage statistics."""
    cfg = _load_config(ctx)
    json_mode = ctx.obj.get('json')

    with _get_active_client(cfg) as client:
        try:
            data = client.stats()
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    if json_mode:
        click.echo(format_json(data))
        return

    click.echo(f"Disk Total:     {_human_size(data.get('disk_total_bytes', 0))}")
    click.echo(f"Disk Used:      {_human_size(data.get('disk_used_bytes', 0))}")
    click.echo(f"Disk Available: {_human_size(data.get('disk_available_bytes', 0))}")
    click.echo(f"Total Clones:   {data.get('clone_count_total', 0)}")

    by_ns = data.get("clones_by_namespace", {})
    if by_ns:
        click.echo("\nClones by Namespace:")
        for ns, count in sorted(by_ns.items()):
            click.echo(f"  {ns}: {count}")
