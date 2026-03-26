"""Connection management commands for cow-cli."""
import sys

import click

from cow_cli.config import (
    AliasValidationError,
    ConnectionConfig,
    DuplicateAliasError,
    InvalidConfigError,
    InvalidURLError,
)
from cow_cli.output import format_json, format_table


def _get_config(ctx) -> ConnectionConfig:
    """Load ConnectionConfig from context, handling errors."""
    config_dir = ctx.obj.get('config_dir')
    cfg = ConnectionConfig(config_dir=config_dir)
    try:
        cfg.load()
    except InvalidConfigError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    return cfg


@click.command()
@click.argument('alias')
@click.argument('url')
@click.option('--token', required=True, help='API token for authentication')
@click.pass_context
def connect(ctx, alias, url, token):
    """Register a new daemon connection."""
    cfg = _get_config(ctx)
    try:
        cfg.add(alias, url, token)
    except DuplicateAliasError:
        click.echo(
            f"Error: Connection '{alias}' already exists. "
            f"Use 'cow-cli update {alias} --token <token>' to update the token.",
            err=True,
        )
        sys.exit(1)
    except AliasValidationError:
        click.echo(
            f"Error: Invalid alias '{alias}' -- aliases may only contain "
            "letters, numbers, hyphens, and underscores.",
            err=True,
        )
        sys.exit(1)
    except InvalidURLError:
        click.echo(
            f"Error: Invalid URL '{url}' -- URL must begin with http:// or https://.",
            err=True,
        )
        sys.exit(1)

    msg = f"Connected '{alias}' -> {cfg.connections[alias]['url']}"
    if cfg.active == alias:
        msg += " (active)"
    click.echo(msg)


@click.command()
@click.pass_context
def connections(ctx):
    """List all registered daemon connections."""
    cfg = _get_config(ctx)
    conn_list = cfg.list_connections()

    if ctx.obj.get('json'):
        # Strip tokens from JSON output for safety
        safe_list = [
            {"alias": c["alias"], "url": c["url"], "active": c["active"]}
            for c in conn_list
        ]
        click.echo(format_json(safe_list))
        return

    if not conn_list:
        click.echo("No connections registered. Run 'cow-cli connect <alias> <url> --token <token>' to add one.")
        return

    headers = ["ALIAS", "URL", "ACTIVE"]
    rows = [
        [c["alias"], c["url"], "*" if c["active"] else ""]
        for c in conn_list
    ]
    click.echo(format_table(headers, rows))


@click.command()
@click.argument('alias_or_url')
@click.pass_context
def activate(ctx, alias_or_url):
    """Activate a daemon connection by alias or URL."""
    cfg = _get_config(ctx)
    try:
        cfg.activate(alias_or_url)
    except KeyError:
        click.echo(
            f"Error: Connection '{alias_or_url}' not found. "
            "Run 'cow-cli connections' to see registered connections.",
            err=True,
        )
        sys.exit(1)
    click.echo(f"Activated '{cfg.active}' -> {cfg.connections[cfg.active]['url']}")


@click.command()
@click.argument('alias')
@click.option('--token', required=True, help='New API token')
@click.pass_context
def update(ctx, alias, token):
    """Update the API token for an existing connection."""
    cfg = _get_config(ctx)
    try:
        cfg.update_token(alias, token)
    except KeyError:
        click.echo(
            f"Error: Connection '{alias}' not found. "
            "Run 'cow-cli connections' to see registered connections.",
            err=True,
        )
        sys.exit(1)
    click.echo(f"Updated token for '{alias}'.")


@click.command()
@click.argument('alias')
@click.pass_context
def disconnect(ctx, alias):
    """Remove a registered daemon connection."""
    cfg = _get_config(ctx)
    was_active = cfg.active == alias
    try:
        cfg.remove(alias)
    except KeyError:
        click.echo(
            f"Error: Connection '{alias}' not found. "
            "Run 'cow-cli connections' to see registered connections.",
            err=True,
        )
        sys.exit(1)
    click.echo(f"Disconnected '{alias}'.")
    if was_active:
        click.echo(
            f"Warning: Removed active connection '{alias}'. "
            "No active connection remains. Run 'cow-cli activate <alias>' to set one.",
            err=True,
        )
