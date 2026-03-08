"""Shared helpers for query and approve commands."""

from __future__ import annotations

import json
import sys

import click

from dbastion.adapters._base import ConnectionConfig, DatabaseType
from dbastion.cli._output import format_execution_result, format_result, render_estimate
from dbastion.connections import get_connection

AUTO_LABELS = {"tool": "dbastion"}

# Default cost threshold (GB).  Overridden by connection config.
_DEFAULT_MAX_GB = 100.0


def resolve_thresholds(
    config: ConnectionConfig,
) -> tuple[float | None, float | None, float | None]:
    """Read cost thresholds from connection config, falling back to defaults.

    Returns (max_gb, max_usd, max_rows). Thresholds can be raised but not
    disabled — values <= 0 are ignored.
    """
    max_gb = config.max_gb if config.max_gb is not None and config.max_gb > 0 else _DEFAULT_MAX_GB
    max_usd = config.max_usd if config.max_usd is not None and config.max_usd > 0 else None
    max_rows = config.max_rows if config.max_rows is not None and config.max_rows > 0 else None

    return max_gb, max_usd, max_rows


def resolve_sql_stdin(sql: str | None, from_stdin: bool) -> str:
    """Resolve SQL from positional argument or stdin. Exactly one source required."""
    if sql and from_stdin:
        raise click.UsageError("Provide SQL as an argument or --from-stdin, not both.")
    if from_stdin:
        if sys.stdin.isatty():
            raise click.UsageError("--from-stdin requires piped input (stdin is a terminal).")
        text = sys.stdin.read().strip()
        if not text:
            raise click.UsageError("--from-stdin: stdin was empty.")
        return text
    if not sql:
        raise click.UsageError("Missing argument 'SQL'. Provide SQL or use --from-stdin.")
    return sql


def parse_db(value: str) -> ConnectionConfig:
    """Resolve --db value: try named connection first, fall back to 'type:key=val' format."""
    # Named connection from ~/.dbastion/connections.toml
    try:
        config = get_connection(value)
    except ValueError as e:
        raise click.BadParameter(str(e), param_hint="'--db'") from e
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


def emit_output(
    output_format: str,
    policy_result: object,
    *,
    estimate: object | None = None,
    exec_result: object | None = None,
    cost_blocked: bool = False,
    cost_diag: object | None = None,
    dry_run_only: bool = False,
    decision: str = "allow",
    db: str | None = None,
) -> None:
    """Emit a single output document (JSON or text)."""
    from dbastion.adapters._base import CostEstimate, ExecutionResult
    from dbastion.diagnostics.render import render_json
    from dbastion.diagnostics.types import DiagnosticResult

    if not isinstance(policy_result, DiagnosticResult):
        raise TypeError(f"expected DiagnosticResult, got {type(policy_result).__name__}")

    if output_format == "json":
        envelope: dict[str, object] = {"decision": decision}
        envelope.update(render_json(policy_result))
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
            envelope["cost_error"] = cost_diag.message
        if isinstance(exec_result, ExecutionResult):
            envelope["columns"] = exec_result.columns
            envelope["rows"] = exec_result.rows
            envelope["row_count"] = exec_result.row_count
            envelope["duration_ms"] = exec_result.duration_ms
        if dry_run_only:
            envelope["dry_run"] = True
        # Include approval info for ask decisions
        if decision == "ask" and db:
            envelope["db"] = db
            envelope["approval_hint"] = (
                "pipe this result to `dbastion approve` to execute"
            )
        click.echo(json.dumps(envelope, indent=2, default=str))
    else:
        # Text: decision header for ask/deny
        if decision == "ask":
            click.echo("decision: ask")
            if cost_blocked and cost_diag is not None:
                click.echo(f"  reason: {cost_diag.message}")
            click.echo(
                "  to approve: rerun with | dbastion approve"
            )
        elif decision == "deny":
            click.echo("decision: deny")
        # Text: policy diagnostics
        output = format_result(policy_result, output_format="text")
        if output:
            click.echo(output)
        # Text: estimate
        if isinstance(estimate, CostEstimate):
            click.echo(render_estimate(estimate))
        if cost_blocked and cost_diag is not None:
            return
        if dry_run_only:
            return
        # Text: execution result
        if isinstance(exec_result, ExecutionResult):
            click.echo(format_execution_result(exec_result, output_format="text"))


async def execute_and_emit(
    config: ConnectionConfig,
    effective_sql: str,
    *,
    adapter: object | None = None,
    original_sql: str | None = None,
    tables: list[str] | None = None,
    dialect: str | None = None,
    decision_label: str = "approved",
    policy_result: object | None = None,
    estimate: object | None = None,
    output_format: str = "json",
) -> int:
    """Execute SQL, emit result, and log. Returns exit code.

    Shared execution path used by both ``query`` (for allowed reads) and
    ``approve`` (for human-approved queries).

    If *adapter* is provided, uses it and leaves it open (caller manages
    lifecycle).  Otherwise connects a fresh adapter and closes it after.

    When *policy_result* is provided, emits the rich envelope (with
    diagnostics, estimate, etc.) via ``emit_output``.  Otherwise emits a
    simple JSON result.
    """
    from dbastion.adapters._registry import get_adapter
    from dbastion.querylog import log_query

    owns_adapter = adapter is None
    if owns_adapter:
        adapter_cls = get_adapter(config.db_type)
        adapter = adapter_cls()
        await adapter.connect(config)

    try:
        exec_result = await adapter.execute(effective_sql, labels=AUTO_LABELS)

        if policy_result is not None:
            emit_output(
                output_format, policy_result, estimate=estimate,
                exec_result=exec_result, decision=decision_label,
            )
        else:
            result: dict[str, object] = {
                "decision": decision_label,
                "status": "success",
                "effective_sql": effective_sql,
                "columns": exec_result.columns,
                "rows": exec_result.rows,
                "row_count": exec_result.row_count,
                "duration_ms": exec_result.duration_ms,
            }
            click.echo(json.dumps(result, indent=2, default=str))

        log_query(
            sql=original_sql or effective_sql,
            effective_sql=effective_sql,
            db=config.name,
            dialect=dialect,
            tables=tables or [],
            blocked=False,
            decision=decision_label,
            cost_gb=exec_result.cost.estimated_gb if exec_result.cost else None,
            cost_usd=exec_result.cost.estimated_cost_usd if exec_result.cost else None,
            duration_ms=exec_result.duration_ms,
            labels=AUTO_LABELS,
        )
    finally:
        if owns_adapter:
            await adapter.close()

    return 0
