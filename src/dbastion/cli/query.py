"""The `query` command: policy engine → dry-run → execute pipeline.

Reads are executed directly. Writes (DML/DDL) are validated and dry-run
only — use `dbastion exec` to execute writes.
"""

from __future__ import annotations

import asyncio
import json

import click

from dbastion.adapters._base import AdapterError, ConnectionConfig
from dbastion.adapters._registry import get_adapter
from dbastion.adapters.cost import check_cost_threshold
from dbastion.cli._output import format_result
from dbastion.cli._shared import AUTO_LABELS, emit_output, parse_db, resolve_sql_stdin
from dbastion.diagnostics import Diagnostic, codes
from dbastion.policy import run_policy
from dbastion.querylog import cleanup_old_logs, log_query

_WRITE_CLASSIFICATIONS = {"dml", "ddl"}


async def _run_query(
    sql: str,
    config: ConnectionConfig,
    *,
    dry_run_only: bool,
    skip_dry_run: bool,
    dialect: str | None,
    limit: int | None,
    max_gb: float | None,
    max_usd: float | None,
    max_rows: float | None,
    output_format: str,
) -> int:
    """Run the full pipeline: policy → dry-run → execute. Returns exit code."""
    # Step 1: Policy engine (allow_write=True — access control is at command level)
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

    # Step 2: Connect adapter
    adapter = adapter_cls()
    await adapter.connect(config)

    has_cost_thresholds = max_gb is not None or max_usd is not None or max_rows is not None

    try:
        # Step 3: Dry-run for cost estimation
        # Adapters return None when EXPLAIN is unsupported (e.g. DDL on Postgres).
        estimate = None
        cost_diag = None
        want_dry_run = is_write or not skip_dry_run or dry_run_only
        if want_dry_run:
            estimate = await adapter.dry_run(policy_result.effective_sql)
            if estimate is not None:
                cost_diag = check_cost_threshold(
                    estimate, max_gb=max_gb, max_usd=max_usd, max_rows=max_rows,
                )
            elif has_cost_thresholds:
                # Thresholds requested but adapter can't estimate → deny.
                cost_diag = Diagnostic.error(
                    codes.COST_OVER_THRESHOLD,
                    "cost thresholds requested but database cannot estimate "
                    "cost for this statement type",
                )

        # Cost threshold exceeded → deny
        if cost_diag is not None:
            emit_output(
                output_format, policy_result, estimate=estimate,
                cost_blocked=True, cost_diag=cost_diag, decision="deny",
            )
            log_query(
                sql=sql,
                effective_sql=policy_result.effective_sql,
                db=config.name,
                dialect=dialect,
                tables=policy_result.tables,
                blocked=True,
                diagnostics=[str(d.code) for d in policy_result.diagnostics],
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
            )
            log_query(
                sql=sql,
                effective_sql=policy_result.effective_sql,
                db=config.name,
                dialect=dialect,
                tables=policy_result.tables,
                blocked=False,
                diagnostics=[str(d.code) for d in policy_result.diagnostics],
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
                diagnostics=[str(d.code) for d in policy_result.diagnostics],
                dry_run=True,
                cost_gb=estimate.estimated_gb if estimate else None,
                cost_usd=estimate.estimated_cost_usd if estimate else None,
                labels=AUTO_LABELS,
            )
            return 0

        # Step 4: Execute (reads only)
        exec_result = await adapter.execute(
            policy_result.effective_sql, labels=AUTO_LABELS,
        )

        emit_output(
            output_format, policy_result, estimate=estimate,
            exec_result=exec_result, decision="allow",
        )

        log_query(
            sql=sql,
            effective_sql=policy_result.effective_sql,
            db=config.name,
            dialect=dialect,
            tables=policy_result.tables,
            blocked=False,
            diagnostics=[str(d.code) for d in policy_result.diagnostics],
            cost_gb=exec_result.cost.estimated_gb if exec_result.cost else None,
            cost_usd=exec_result.cost.estimated_cost_usd if exec_result.cost else None,
            duration_ms=exec_result.duration_ms,
            labels=AUTO_LABELS,
        )
    finally:
        await adapter.close()

    return 0


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
@click.option("--no-limit", is_flag=True, help="Disable auto-LIMIT injection.")
@click.option("--dry-run", is_flag=True, help="Estimate cost only, do not execute.")
@click.option("--skip-dry-run", is_flag=True, help="Skip cost estimation, execute directly.")
@click.option(
    "--max-gb", type=float, default=69,
    help="Block if scan exceeds N GB (default: 69, 0 to disable).",
)
@click.option("--max-usd", type=float, default=None, help="Block if cost exceeds $N (BigQuery).")
@click.option("--max-rows", type=float, default=None, help="Block if rows exceed N.")
def query(
    sql: str | None,
    from_stdin: bool,
    db: str,
    dialect: str | None,
    output_format: str,
    limit: int,
    no_limit: bool,
    dry_run: bool,
    skip_dry_run: bool,
    max_gb: float | None,
    max_usd: float | None,
    max_rows: float | None,
) -> None:
    """Execute a guarded SQL query.

    Reads are executed directly. Writes (DML/DDL) are validated and
    cost-estimated but not executed — use `dbastion exec` to run them.
    """
    cleanup_old_logs()
    sql = resolve_sql_stdin(sql, from_stdin)
    effective_limit = None if no_limit else (limit if limit > 0 else None)
    # --max-gb 0 disables the threshold.
    if max_gb is not None and max_gb <= 0:
        max_gb = None

    try:
        config = parse_db(db)
    except click.BadParameter as e:
        click.echo(f"error: {e.format_message()}", err=True)
        raise SystemExit(1) from e

    try:
        # Use adapter's dialect if none specified.
        if dialect is None:
            adapter_cls = get_adapter(config.db_type)
            dialect = adapter_cls().dialect()

        exit_code = asyncio.run(
            _run_query(
                sql,
                config,
                dry_run_only=dry_run,
                skip_dry_run=skip_dry_run,
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
