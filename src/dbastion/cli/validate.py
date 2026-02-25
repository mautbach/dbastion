"""The `validate` command: check SQL through the policy engine without executing."""

from __future__ import annotations

import click

from dbastion.cli._output import format_result
from dbastion.policy import run_policy


@click.command()
@click.argument("sql")
@click.option("--dialect", default=None, help="SQL dialect (postgres, bigquery, mysql, etc.)")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "text"]),
    default="text",
    help="Output format.",
)
@click.option("--limit", type=int, default=1000, help="Auto-LIMIT value (0 to disable).")
@click.option("--allow-write", is_flag=True, help="Allow DML/DDL statements.")
def validate(
    sql: str,
    dialect: str | None,
    output_format: str,
    limit: int,
    allow_write: bool,
) -> None:
    """Validate SQL through the policy engine without executing."""
    effective_limit = limit if limit > 0 else None
    result = run_policy(sql, dialect=dialect, allow_write=allow_write, limit=effective_limit)
    output = format_result(result, output_format=output_format)
    if output:
        click.echo(output)
    if result.blocked:
        raise SystemExit(1)
