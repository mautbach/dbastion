"""The `schema` command: drill-down introspection (schemas → tables → columns)."""

from __future__ import annotations

import json

import click

from dbastion.adapters._base import AdapterError
from dbastion.adapters._registry import get_adapter
from dbastion.cli._shared import parse_db


@click.group("schema")
def schema() -> None:
    """Browse database schemas, tables, and columns."""


@schema.command("ls")
@click.argument("schema_name", required=False, default=None)
@click.option("--db", required=True, envvar="DBASTION_DB", help="Connection name or type:key=val.")
@click.option("--format", "output_format", type=click.Choice(["json", "text"]), default="json")
def ls(schema_name: str | None, db: str, output_format: str) -> None:
    """List schemas, or tables within a schema."""
    config = parse_db(db)

    try:
        adapter_cls = get_adapter(config.db_type)
        adapter = adapter_cls()
        import asyncio

        asyncio.run(adapter.connect(config))
        try:
            if schema_name:
                tables = asyncio.run(adapter.list_tables(schema_name))
                if output_format == "json":
                    click.echo(json.dumps({
                        "schema": schema_name,
                        "tables": [t.name for t in tables],
                    }, indent=2))
                else:
                    if not tables:
                        click.echo(f"No tables in '{schema_name}'.")
                    else:
                        for t in tables:
                            click.echo(t.name)
            else:
                schemas = asyncio.run(adapter.list_schemas())
                if output_format == "json":
                    click.echo(json.dumps({"schemas": schemas}, indent=2))
                else:
                    if not schemas:
                        click.echo("No schemas found.")
                    else:
                        for s in schemas:
                            click.echo(s)
        finally:
            asyncio.run(adapter.close())
    except AdapterError as e:
        if output_format == "json":
            click.echo(json.dumps({"error": str(e)}, indent=2))
        else:
            click.echo(f"error: {e}", err=True)
        raise SystemExit(1) from None


@schema.command("show")
@click.argument("table_ref")
@click.option("--db", required=True, envvar="DBASTION_DB", help="Connection name or type:key=val.")
@click.option("--format", "output_format", type=click.Choice(["json", "text"]), default="json")
def show(table_ref: str, db: str, output_format: str) -> None:
    """Show columns of a table. TABLE_REF is schema.table or just table."""
    config = parse_db(db)

    if "." in table_ref:
        schema_name, table_name = table_ref.split(".", 1)
    else:
        schema_name, table_name = None, table_ref

    try:
        adapter_cls = get_adapter(config.db_type)
        adapter = adapter_cls()
        import asyncio

        asyncio.run(adapter.connect(config))
        try:
            info = asyncio.run(adapter.describe_table(table_name, schema=schema_name))
            if output_format == "json":
                doc: dict[str, object] = {
                    "schema": info.schema,
                    "table": info.name,
                }
                if info.row_count_estimate is not None:
                    doc["row_count_estimate"] = info.row_count_estimate
                if info.metadata:
                    doc["metadata"] = info.metadata
                doc["columns"] = [
                    {
                        "name": c.name,
                        "type": c.data_type,
                        "nullable": c.is_nullable,
                        **({"comment": c.comment} if c.comment else {}),
                    }
                    for c in info.columns
                ]
                click.echo(json.dumps(doc, indent=2))
            else:
                click.echo(f"{info.schema}.{info.name}")
                if info.row_count_estimate is not None:
                    click.echo(f"  rows: ~{info.row_count_estimate}")
                if info.metadata:
                    for k, v in info.metadata.items():
                        click.echo(f"  {k}: {v}")
                for c in info.columns:
                    nullable = "NULL" if c.is_nullable else "NOT NULL"
                    line = f"  {c.name}  {c.data_type}  {nullable}"
                    if c.comment:
                        line += f"  -- {c.comment}"
                    click.echo(line)
        finally:
            asyncio.run(adapter.close())
    except AdapterError as e:
        if output_format == "json":
            click.echo(json.dumps({"error": str(e)}, indent=2))
        else:
            click.echo(f"error: {e}", err=True)
        raise SystemExit(1) from None
