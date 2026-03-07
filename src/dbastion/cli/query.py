"""The `query` command: policy engine → dry-run → execute pipeline.

Reads are executed directly. Writes (DML/DDL) return decision: ask —
pipe through `dbastion approve` to execute.
"""

from __future__ import annotations

import asyncio
import json

import click

from dbastion.adapters._base import AdapterError, ConnectionConfig
from dbastion.adapters._registry import get_adapter
from dbastion.adapters.cost import check_cost_threshold
from dbastion.cli._output import format_result
from dbastion.cli._shared import (
    AUTO_LABELS,
    emit_output,
    execute_and_emit,
    parse_db,
    resolve_sql_stdin,
    resolve_thresholds,
)
from dbastion.policy import run_policy
from dbastion.querylog import cleanup_old_logs, log_query

_WRITE_CLASSIFICATIONS = {"dml", "ddl"}


async def _run_query(
    sql: str,
    config: ConnectionConfig,
    *,
    db_raw: str,
    dry_run_only: bool,
    dialect: str | None,
    limit: int | None,
    max_gb: float | None,
    max_usd: float | None,
    max_rows: float | None,
    output_format: str,
) -> int:
    """Run the full pipeline: policy → dry-run → execute. Returns exit code."""
    # Step 1: Policy engine
    adapter_cls = get_adapter(config.db_type)
    dangerous_fns = adapter_cls().dangerous_functions()
    policy_result = run_policy(
        sql, dialect=dialect, allow_write=True, limit=limit,
        dangerous_functions=dangerous_fns,
    )

    is_write = policy_result.classification in _WRITE_CLASSIFICATIONS

    if policy_result.blocked:
        decision = "deny"
        if output_format == "json":
            emit_output(output_format, policy_result, decision=decision)
        else:
            click.echo(format_result(policy_result, output_format=output_format))
        log_query(
            sql=sql,
            effective_sql=policy_result.effective_sql,
            db=config.name,
            dialect=dialect,
            tables=policy_result.tables,
            blocked=True,
            diagnostics=[str(d.code) for d in policy_result.diagnostics],
            dry_run=True,
        )
        return 1

    # Step 2: Connect once — used for both dry-run and execute
    adapter = adapter_cls()
    await adapter.connect(config)

    try:
        if adapter.supports_dry_run_for(policy_result.classification):
            estimate = await adapter.dry_run(policy_result.effective_sql)
        else:
            estimate = None
        cost_diag = None
        if estimate is not None:
            cost_diag = check_cost_threshold(
                estimate, max_gb=max_gb, max_usd=max_usd, max_rows=max_rows,
            )

        # Cost threshold exceeded → ask (human can approve via pipe)
        if cost_diag is not None:
            emit_output(
                output_format, policy_result, estimate=estimate,
                cost_blocked=True, cost_diag=cost_diag, decision="ask",
                db=db_raw,
            )
            log_query(
                sql=sql,
                effective_sql=policy_result.effective_sql,
                db=config.name,
                dialect=dialect,
                tables=policy_result.tables,
                blocked=False,
                dry_run=True,
                cost_gb=estimate.estimated_gb if estimate else None,
                cost_usd=estimate.estimated_cost_usd if estimate else None,
                labels=AUTO_LABELS,
            )
            return 1

        # Writes: validate + dry-run only → ask
        if is_write:
            emit_output(
                output_format, policy_result, estimate=estimate, decision="ask",
                db=db_raw,
            )
            log_query(
                sql=sql,
                effective_sql=policy_result.effective_sql,
                db=config.name,
                dialect=dialect,
                tables=policy_result.tables,
                blocked=False,
                dry_run=True,
                cost_gb=estimate.estimated_gb if estimate else None,
                cost_usd=estimate.estimated_cost_usd if estimate else None,
                labels=AUTO_LABELS,
            )
            return 0

        # --dry-run: stop after estimation, don't execute
        if dry_run_only:
            emit_output(
                output_format, policy_result, estimate=estimate,
                dry_run_only=True, decision="allow",
            )
            log_query(
                sql=sql,
                effective_sql=policy_result.effective_sql,
                db=config.name,
                dialect=dialect,
                tables=policy_result.tables,
                blocked=False,
                dry_run=True,
                cost_gb=estimate.estimated_gb if estimate else None,
                cost_usd=estimate.estimated_cost_usd if estimate else None,
                labels=AUTO_LABELS,
            )
            return 0

        # Step 3: Execute reads — reuses same adapter connection
        return await execute_and_emit(
            config,
            policy_result.effective_sql,
            adapter=adapter,
            original_sql=sql,
            tables=policy_result.tables,
            dialect=dialect,
            decision_label="allow",
            policy_result=policy_result,
            estimate=estimate,
            output_format=output_format,
        )
    finally:
        await adapter.close()


@click.command()
@click.argument("sql", required=False, default=None)
@click.option("--from-stdin", is_flag=True, help="Read SQL from stdin instead of argument.")
@click.option("--db", required=True, envvar="DBASTION_DB", help="Connection name or type:key=val.")
@click.option("--dialect", default=None, help="SQL dialect (postgres, bigquery, duckdb, etc.)")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "text"]),
    default="json",
    help="Output format.",
)
@click.option("--limit", type=int, default=1000, help="Auto-LIMIT value (0 to disable).")
@click.option("--dry-run", is_flag=True, help="Estimate cost only, do not execute.")
def query(
    sql: str | None,
    from_stdin: bool,
    db: str,
    dialect: str | None,
    output_format: str,
    limit: int,
    dry_run: bool,
) -> None:
    """Execute a guarded SQL query.

    Reads are executed directly. Writes (DML/DDL) are validated and
    cost-estimated but not executed — pipe through `dbastion approve`:

    \b
        dbastion query "INSERT INTO ..." --db prod | dbastion approve

    Cost thresholds are configured per-connection in ~/.dbastion/connections.toml.
    If a read exceeds thresholds, the same approve pattern applies:

    \b
        dbastion query "SELECT ..." --db prod | dbastion approve
    """
    cleanup_old_logs()
    sql = resolve_sql_stdin(sql, from_stdin)
    effective_limit = limit if limit > 0 else None

    try:
        config = parse_db(db)
    except click.BadParameter as e:
        click.echo(f"error: {e.format_message()}", err=True)
        raise SystemExit(1) from e

    max_gb, max_usd, max_rows = resolve_thresholds(config)

    try:
        # Use adapter's dialect if none specified.
        if dialect is None:
            adapter_cls = get_adapter(config.db_type)
            dialect = adapter_cls().dialect()

        exit_code = asyncio.run(
            _run_query(
                sql,
                config,
                db_raw=db,
                dry_run_only=dry_run,
                dialect=dialect,
                limit=effective_limit,
                max_gb=max_gb,
                max_usd=max_usd,
                max_rows=max_rows,
                output_format=output_format,
            )
        )
    except AdapterError as e:
        if output_format == "json":
            click.echo(json.dumps({"decision": "deny", "blocked": True, "error": str(e)}, indent=2))
        else:
            click.echo(f"error: {e}", err=True)
        raise SystemExit(1) from e

    if exit_code != 0:
        raise SystemExit(exit_code)
