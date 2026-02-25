"""The `query` command: policy engine → dry-run → execute pipeline."""

from __future__ import annotations

import asyncio
import json

import click

from dbastion.adapters._base import AdapterError, ConnectionConfig, DatabaseType
from dbastion.adapters._registry import get_adapter
from dbastion.adapters.cost import check_cost_threshold
from dbastion.cli._output import format_execution_result, format_result
from dbastion.policy import run_policy
from dbastion.querylog import cleanup_old_logs, log_query

_AUTO_LABELS = {"tool": "dbastion"}


def _parse_db(value: str) -> ConnectionConfig:
    """Parse --db 'type:key=val,key=val' into ConnectionConfig."""
    if ":" not in value:
        raise click.BadParameter(
            f"Expected format 'type:key=val,...' (e.g. 'bigquery:project=my-proj'), got '{value}'",
            param_hint="'--db'",
        )
    db_type_str, params_str = value.split(":", 1)

    try:
        db_type = DatabaseType(db_type_str)
    except ValueError as e:
        valid = ", ".join(t.value for t in DatabaseType)
        raise click.BadParameter(
            f"Unknown database type '{db_type_str}'. Valid: {valid}",
            param_hint="'--db'",
        ) from e

    params: dict[str, str] = {}
    if params_str:
        for part in params_str.split(","):
            if "=" not in part:
                raise click.BadParameter(
                    f"Expected key=value pair, got '{part}'",
                    param_hint="'--db'",
                )
            k, v = part.split("=", 1)
            params[k.strip()] = v.strip()

    return ConnectionConfig(name=db_type_str, db_type=db_type, params=params)


async def _run_query(
    sql: str,
    config: ConnectionConfig,
    *,
    dry_run_only: bool,
    skip_dry_run: bool,
    dialect: str | None,
    allow_write: bool,
    limit: int | None,
    max_gb: float | None,
    max_usd: float | None,
    max_rows: float | None,
    output_format: str,
) -> int:
    """Run the full pipeline: policy → dry-run → execute. Returns exit code."""
    # Step 1: Policy engine
    result = run_policy(sql, dialect=dialect, allow_write=allow_write, limit=limit)
    output = format_result(result, output_format=output_format)
    if output:
        click.echo(output)
    if result.blocked:
        log_query(
            sql=sql,
            effective_sql=result.effective_sql,
            db=config.name,
            dialect=dialect,
            tables=result.tables,
            blocked=True,
            diagnostics=[str(d.code) for d in result.diagnostics],
            dry_run=True,
        )
        return 1

    # Step 2: Load adapter + connect
    adapter_cls = get_adapter(config.db_type)
    adapter = adapter_cls()
    await adapter.connect(config)

    try:
        # Step 3: Dry-run for cost estimation (default — always runs unless --skip-dry-run)
        if not skip_dry_run:
            estimate = await adapter.dry_run(result.effective_sql)
            cost_diag = check_cost_threshold(
                estimate, max_gb=max_gb, max_usd=max_usd, max_rows=max_rows,
            )

            # Output estimate
            if output_format == "json":
                estimate_data: dict[str, object] = {
                    "dry_run": True,
                    "estimate": {
                        "summary": estimate.summary,
                    },
                }
                if estimate.estimated_gb is not None:
                    estimate_data["estimate"]["estimated_gb"] = estimate.estimated_gb  # type: ignore[index]
                    estimate_data["estimate"]["estimated_cost_usd"] = estimate.estimated_cost_usd  # type: ignore[index]
                if estimate.estimated_rows is not None:
                    estimate_data["estimate"]["estimated_rows"] = estimate.estimated_rows  # type: ignore[index]
                if estimate.plan_node:
                    estimate_data["estimate"]["plan"] = estimate.plan_node  # type: ignore[index]
                if estimate.warnings:
                    estimate_data["estimate"]["warnings"] = estimate.warnings  # type: ignore[index]
                estimate_data["blocked"] = cost_diag is not None
                click.echo(json.dumps(estimate_data, indent=2))
            else:
                click.echo(f"\nestimate: {estimate.summary}")
                for w in estimate.warnings:
                    click.echo(f"  warning: {w}")

            # Gate: block if over threshold
            if cost_diag is not None:
                click.echo(f"\nerror[{cost_diag.code}]: {cost_diag.message}")
                for note in cost_diag.notes:
                    click.echo(f"  = note: {note}")
                log_query(
                    sql=sql,
                    effective_sql=result.effective_sql,
                    db=config.name,
                    dialect=dialect,
                    tables=result.tables,
                    blocked=True,
                    diagnostics=[str(d.code) for d in result.diagnostics],
                    dry_run=True,
                    cost_gb=estimate.estimated_gb,
                    cost_usd=estimate.estimated_cost_usd,
                    labels=_AUTO_LABELS,
                )
                return 1

            # --dry-run: stop after estimation, don't execute
            if dry_run_only:
                log_query(
                    sql=sql,
                    effective_sql=result.effective_sql,
                    db=config.name,
                    dialect=dialect,
                    tables=result.tables,
                    blocked=False,
                    diagnostics=[str(d.code) for d in result.diagnostics],
                    dry_run=True,
                    cost_gb=estimate.estimated_gb,
                    cost_usd=estimate.estimated_cost_usd,
                    labels=_AUTO_LABELS,
                )
                return 0

        # Step 4: Execute
        exec_result = await adapter.execute(result.effective_sql, labels=_AUTO_LABELS)
        output = format_execution_result(exec_result, output_format=output_format)
        click.echo(output)

        log_query(
            sql=sql,
            effective_sql=result.effective_sql,
            db=config.name,
            dialect=dialect,
            tables=result.tables,
            blocked=False,
            diagnostics=[str(d.code) for d in result.diagnostics],
            cost_gb=exec_result.cost.estimated_gb if exec_result.cost else None,
            cost_usd=exec_result.cost.estimated_cost_usd if exec_result.cost else None,
            duration_ms=exec_result.duration_ms,
            labels=_AUTO_LABELS,
        )
    finally:
        await adapter.close()

    return 0


@click.command()
@click.argument("sql")
@click.option("--db", required=True, help="Database connection (type:key=val,...).")
@click.option("--dialect", default=None, help="SQL dialect (postgres, bigquery, duckdb, etc.)")
@click.option("--allow-write", is_flag=True, help="Allow DML/DDL statements.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "text"]),
    default="json",
    help="Output format.",
)
@click.option("--limit", type=int, default=1000, help="Auto-LIMIT value (0 to disable).")
@click.option("--no-limit", is_flag=True, help="Disable auto-LIMIT injection.")
@click.option("--dry-run", is_flag=True, help="Estimate cost only, do not execute.")
@click.option("--skip-dry-run", is_flag=True, help="Skip cost estimation, execute directly.")
@click.option("--max-gb", type=float, default=None, help="Block if scan exceeds N GB (BigQuery).")
@click.option("--max-usd", type=float, default=None, help="Block if cost exceeds $N (BigQuery).")
@click.option("--max-rows", type=float, default=None, help="Block if rows exceed N.")
def query(
    sql: str,
    db: str,
    dialect: str | None,
    allow_write: bool,
    output_format: str,
    limit: int,
    no_limit: bool,
    dry_run: bool,
    skip_dry_run: bool,
    max_gb: float | None,
    max_usd: float | None,
    max_rows: float | None,
) -> None:
    """Execute a guarded SQL query."""
    cleanup_old_logs()
    effective_limit = None if no_limit else (limit if limit > 0 else None)

    try:
        config = _parse_db(db)
    except click.BadParameter as e:
        click.echo(f"error: {e.format_message()}", err=True)
        raise SystemExit(1) from e

    # Use adapter's dialect if none specified.
    if dialect is None:
        adapter_cls = get_adapter(config.db_type)
        dialect = adapter_cls().dialect()

    try:
        exit_code = asyncio.run(
            _run_query(
                sql,
                config,
                dry_run_only=dry_run,
                skip_dry_run=skip_dry_run,
                dialect=dialect,
                allow_write=allow_write,
                limit=effective_limit,
                max_gb=max_gb,
                max_usd=max_usd,
                max_rows=max_rows,
                output_format=output_format,
            )
        )
    except AdapterError as e:
        click.echo(f"error: {e}", err=True)
        raise SystemExit(1) from e

    if exit_code != 0:
        raise SystemExit(exit_code)
