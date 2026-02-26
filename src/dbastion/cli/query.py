"""The `query` command: policy engine → dry-run → execute pipeline."""

from __future__ import annotations

import asyncio
import json

import click

from dbastion.adapters._base import AdapterError, ConnectionConfig, DatabaseType
from dbastion.adapters._registry import get_adapter
from dbastion.adapters.cost import check_cost_threshold
from dbastion.cli._output import format_execution_result, format_result, render_estimate
from dbastion.connections import get_connection
from dbastion.policy import run_policy
from dbastion.querylog import cleanup_old_logs, log_query

_AUTO_LABELS = {"tool": "dbastion"}


def _parse_db(value: str) -> ConnectionConfig:
    """Resolve --db value: try named connection first, fall back to 'type:key=val' format."""
    # Named connection from ~/.dbastion/connections.toml
    config = get_connection(value)
    if config is not None:
        return config

    # Raw format: type:key=val,key=val
    if ":" not in value:
        raise click.BadParameter(
            f"Connection '{value}' not found in ~/.dbastion/connections.toml "
            f"and not in 'type:key=val' format.\n"
            f"  Add it: dbastion connect add {value} <type> <param>=<val>",
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
    adapter_cls = get_adapter(config.db_type)
    dangerous_fns = adapter_cls().dangerous_functions()
    policy_result = run_policy(
        sql, dialect=dialect, allow_write=allow_write, limit=limit,
        dangerous_functions=dangerous_fns,
    )

    if policy_result.blocked:
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

    try:
        # Step 3: Dry-run for cost estimation
        # --dry-run always triggers estimation, even with --skip-dry-run
        estimate = None
        cost_diag = None
        if not skip_dry_run or dry_run_only:
            estimate = await adapter.dry_run(policy_result.effective_sql)
            cost_diag = check_cost_threshold(
                estimate, max_gb=max_gb, max_usd=max_usd, max_rows=max_rows,
            )

        # Emit output: blocked by cost threshold
        if cost_diag is not None:
            _emit_output(
                output_format, policy_result, estimate=estimate,
                cost_blocked=True, cost_diag=cost_diag,
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
                labels=_AUTO_LABELS,
            )
            return 1

        # --dry-run: stop after estimation, don't execute
        if dry_run_only:
            _emit_output(
                output_format, policy_result, estimate=estimate,
                dry_run_only=True,
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
                labels=_AUTO_LABELS,
            )
            return 0

        # Step 4: Execute
        exec_result = await adapter.execute(
            policy_result.effective_sql, labels=_AUTO_LABELS,
        )

        _emit_output(
            output_format, policy_result, estimate=estimate,
            exec_result=exec_result,
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
            labels=_AUTO_LABELS,
        )
    finally:
        await adapter.close()

    return 0


def _emit_output(
    output_format: str,
    policy_result: object,
    *,
    estimate: object | None = None,
    exec_result: object | None = None,
    cost_blocked: bool = False,
    cost_diag: object | None = None,
    dry_run_only: bool = False,
) -> None:
    """Emit a single output document (JSON or text)."""
    from dbastion.adapters._base import CostEstimate, ExecutionResult
    from dbastion.diagnostics.render import render_json
    from dbastion.diagnostics.types import DiagnosticResult

    assert isinstance(policy_result, DiagnosticResult)

    if output_format == "json":
        envelope: dict[str, object] = render_json(policy_result)
        if isinstance(estimate, CostEstimate):
            est_data: dict[str, object] = {"summary": estimate.summary}
            if estimate.estimated_gb is not None:
                est_data["estimated_gb"] = estimate.estimated_gb
                est_data["estimated_cost_usd"] = estimate.estimated_cost_usd
            if estimate.estimated_rows is not None:
                est_data["estimated_rows"] = estimate.estimated_rows
            if estimate.plan_node:
                est_data["plan"] = estimate.plan_node
            if estimate.warnings:
                est_data["warnings"] = estimate.warnings
            envelope["estimate"] = est_data
        if cost_blocked and cost_diag is not None:
            envelope["blocked"] = True
            envelope["cost_error"] = str(cost_diag)
        if isinstance(exec_result, ExecutionResult):
            envelope["columns"] = exec_result.columns
            envelope["rows"] = exec_result.rows
            envelope["row_count"] = exec_result.row_count
            envelope["duration_ms"] = exec_result.duration_ms
        if dry_run_only:
            envelope["dry_run"] = True
        click.echo(json.dumps(envelope, indent=2, default=str))
    else:
        # Text: policy diagnostics
        output = format_result(policy_result, output_format="text")
        if output:
            click.echo(output)
        # Text: estimate
        if isinstance(estimate, CostEstimate):
            click.echo(render_estimate(estimate))
        if cost_blocked and cost_diag is not None:
            click.echo(f"\nerror: {cost_diag}")
            return
        if dry_run_only:
            return
        # Text: execution result
        if isinstance(exec_result, ExecutionResult):
            click.echo(format_execution_result(exec_result, output_format="text"))


@click.command()
@click.argument("sql")
@click.option("--db", required=True, envvar="DBASTION_DB", help="Connection name or type:key=val.")
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
