"""The `connect` command group: manage named database connections."""

from __future__ import annotations

import re

import click

from dbastion.adapters._base import DatabaseType
from dbastion.connections import list_connections, remove_connection, save_connection

_PASSWORD_RE = re.compile(r"(://[^:]+:)[^@]+(@)")


def _mask_secrets(value: str) -> str:
    """Mask passwords in DSN-style connection strings."""
    return _PASSWORD_RE.sub(r"\1****\2", value)


@click.group()
def connect() -> None:
    """Manage named database connections (~/.dbastion/connections.toml)."""


@connect.command("add")
@click.argument("name")
@click.argument("db_type", type=click.Choice([t.value for t in DatabaseType]))
@click.argument("params", nargs=-1, required=True)
def connect_add(name: str, db_type: str, params: tuple[str, ...]) -> None:
    """Add a named connection.

    \b
    Examples:
      dbastion connect add tpch postgres dsn=postgresql://user:pass@host:5432/db
      dbastion connect add prod bigquery project=my-project location=US
      dbastion connect add local duckdb path=:memory:
    """
    parsed: dict[str, str] = {}
    for p in params:
        if "=" not in p:
            raise click.BadParameter(f"Expected key=value, got '{p}'")
        k, v = p.split("=", 1)
        parsed[k] = v

    path = save_connection(name, db_type, parsed)
    click.echo(f"Saved connection '{name}' to {path}")


@connect.command("list")
def connect_list() -> None:
    """List all named connections."""
    connections = list_connections()
    if not connections:
        click.echo("No connections configured.")
        click.echo("Add one: dbastion connect add <name> <type> <param>=<val>")
        return

    for name, entry in connections.items():
        db_type = entry.get("type", "?")
        params = {k: v for k, v in entry.items() if k != "type"}
        param_str = ", ".join(
            f"{k}={_mask_secrets(str(v))}" for k, v in params.items()
        )
        click.echo(f"  {name} ({db_type}): {param_str}")


@connect.command("remove")
@click.argument("name")
def connect_remove(name: str) -> None:
    """Remove a named connection."""
    if not remove_connection(name):
        click.echo(f"Connection '{name}' not found.", err=True)
        raise SystemExit(1)
    click.echo(f"Removed connection '{name}'.")
