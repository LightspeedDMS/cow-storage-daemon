"""Clone lifecycle and job status commands for cow-cli."""
import sys

import click

from cow_cli.client import CLIError, CowClient
from cow_cli.config import ConnectionConfig, InvalidConfigError
from cow_cli.output import format_json, format_table


_SPINNER_CHARS = "|/-\\"


def _get_client(ctx) -> CowClient:
    """Build CowClient from active connection."""
    config_dir = ctx.obj.get('config_dir')
    cfg = ConnectionConfig(config_dir=config_dir)
    try:
        cfg.load()
    except InvalidConfigError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

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


@click.command("clone")
@click.argument("source_path")
@click.option("--namespace", required=True, help="Clone namespace")
@click.option("--name", required=True, help="Clone name")
@click.option("--nowait", is_flag=True, default=False,
              help="Return immediately without waiting for completion")
@click.option("--timeout", type=float, default=300.0,
              help="Maximum seconds to wait (default: 300)")
@click.pass_context
def clone_cmd(ctx, source_path, namespace, name, nowait, timeout):
    """Create a CoW clone from a source directory."""
    with _get_client(ctx) as client:
        try:
            result = client.create_clone(source_path, namespace, name)
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

        job_id = result.get("job_id", "")

        if nowait:
            if ctx.obj.get('json'):
                click.echo(format_json({"job_id": job_id}))
            else:
                click.echo(f"Job submitted: {job_id}")
                click.echo(f"Check status with: cow-cli job {job_id}")
            return

        # Wait mode (default)
        def spinner(elapsed):
            char = _SPINNER_CHARS[int(elapsed) % len(_SPINNER_CHARS)]
            click.echo(f"\r{char} Cloning... ({int(elapsed)}s)", nl=False, err=True)

        try:
            job = client.wait_for_job(
                job_id, poll_interval=2.0, timeout=timeout,
                spinner_callback=spinner,
            )
        except CLIError as exc:
            click.echo("", err=True)  # Clear spinner line
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

        click.echo("\r", nl=False, err=True)  # Clear spinner line

        if ctx.obj.get('json'):
            click.echo(format_json(job))
        else:
            click.echo(f"Clone created: {job.get('namespace')}/{job.get('name')}")
            clone_path = job.get("clone_path", "")
            if clone_path:
                click.echo(f"Clone path: {clone_path}")


@click.command("list")
@click.option("--namespace", default=None, help="Filter by namespace")
@click.pass_context
def list_cmd(ctx, namespace):
    """List all clones."""
    with _get_client(ctx) as client:
        try:
            clones = client.list_clones(namespace=namespace)
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    if ctx.obj.get('json'):
        click.echo(format_json(clones))
        return

    if not clones:
        click.echo("No clones found.")
        return

    headers = ["NAMESPACE", "NAME", "SOURCE", "CREATED", "SIZE"]
    rows = []
    for c in clones:
        created = c.get("created_at", "")
        if isinstance(created, str) and "T" in created:
            created = created.split("T")[0] + " " + created.split("T")[1][:5]
        rows.append([
            c.get("namespace", ""),
            c.get("name", ""),
            c.get("source_path", ""),
            created,
            _human_size(c.get("size_bytes", 0)),
        ])
    click.echo(format_table(headers, rows))


@click.command("info")
@click.argument("namespace")
@click.argument("name")
@click.pass_context
def info_cmd(ctx, namespace, name):
    """Show details of a specific clone."""
    with _get_client(ctx) as client:
        try:
            clone = client.get_clone(namespace, name)
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    if ctx.obj.get('json'):
        click.echo(format_json(clone))
        return

    click.echo(f"Namespace:   {clone.get('namespace', '')}")
    click.echo(f"Name:        {clone.get('name', '')}")
    click.echo(f"Source:      {clone.get('source_path', '')}")
    click.echo(f"Clone Path:  {clone.get('clone_path', '')}")
    click.echo(f"Size:        {_human_size(clone.get('size_bytes', 0))}")
    click.echo(f"Created:     {clone.get('created_at', '')}")


@click.command("delete")
@click.argument("namespace")
@click.argument("name")
@click.option("--force", is_flag=True, default=False,
              help="Skip confirmation prompt")
@click.pass_context
def delete_cmd(ctx, namespace, name, force):
    """Delete a clone."""
    if not force:
        confirmed = click.confirm(f"Delete clone '{namespace}/{name}'?", default=False)
        if not confirmed:
            click.echo("Cancelled.")
            return

    with _get_client(ctx) as client:
        try:
            client.delete_clone(namespace, name)
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    click.echo(f"Deleted clone '{namespace}/{name}'.")


@click.command("job")
@click.argument("job_id")
@click.pass_context
def job_cmd(ctx, job_id):
    """Check status of an async clone job."""
    with _get_client(ctx) as client:
        try:
            job = client.get_job(job_id)
        except CLIError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)

    if ctx.obj.get('json'):
        click.echo(format_json(job))
        return

    status = job.get("status", "unknown")
    click.echo(f"Job ID:      {job.get('job_id', '')}")
    click.echo(f"Status:      {status}")
    click.echo(f"Namespace:   {job.get('namespace', '')}")
    click.echo(f"Name:        {job.get('name', '')}")
    click.echo(f"Source:      {job.get('source_path', '')}")

    if status == "completed":
        click.echo(f"Clone Path:  {job.get('clone_path', '')}")
        click.echo(f"Completed:   {job.get('completed_at', '')}")
    elif status == "failed":
        click.echo(f"Error:       {job.get('error', 'Unknown')}")
