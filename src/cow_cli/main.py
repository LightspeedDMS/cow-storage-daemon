"""cow-cli — CLI for CoW Storage Daemon."""
import click

from cow_cli import __version__
from cow_cli.commands.connection import connect, connections, activate, update, disconnect
from cow_cli.commands.clone import clone_cmd, list_cmd, info_cmd, delete_cmd, job_cmd
from cow_cli.commands.health import health_cmd, stats_cmd


@click.group()
@click.option('--config-dir', type=click.Path(), default=None, hidden=True,
              help='Override config directory (for testing)')
@click.option('--json', 'json_output', is_flag=True, default=False,
              help='Output as JSON')
@click.version_option(version=__version__, prog_name='cow-cli')
@click.pass_context
def cli(ctx, config_dir, json_output):
    """CLI for CoW Storage Daemon — manage connections, clones, and monitoring."""
    ctx.ensure_object(dict)
    ctx.obj['json'] = json_output
    ctx.obj['config_dir'] = config_dir


cli.add_command(connect)
cli.add_command(connections)
cli.add_command(activate)
cli.add_command(update)
cli.add_command(disconnect)
cli.add_command(clone_cmd)
cli.add_command(list_cmd)
cli.add_command(info_cmd)
cli.add_command(delete_cmd)
cli.add_command(job_cmd)
cli.add_command(health_cmd)
cli.add_command(stats_cmd)
