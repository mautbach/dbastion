"""Shared helpers for query and exec commands."""

from __future__ import annotations

import json
import sys

import click

from dbastion.adapters._base import ConnectionConfig, DatabaseType
from dbastion.cli._output import format_execution_result, format_result, render_estimate
from dbastion.connections import get_connection

AUTO_LABELS = {"tool": "dbastion"}


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
) -> None:
    """Emit a single output document (JSON or text)."""
    from dbastion.adapters._base import CostEstimate, ExecutionResult
    from dbastion.diagnostics.render import render_json
    from dbastion.diagnostics.types import DiagnosticResult

    assert isinstance(policy_result, DiagnosticResult)

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
            envelope["decision"] = "deny"
            envelope["cost_error"] = cost_diag.message
        if isinstance(exec_result, ExecutionResult):
            envelope["columns"] = exec_result.columns
            envelope["rows"] = exec_result.rows
            envelope["row_count"] = exec_result.row_count
            envelope["duration_ms"] = exec_result.duration_ms
        if dry_run_only:
            envelope["dry_run"] = True
        click.echo(json.dumps(envelope, indent=2, default=str))
    else:
        # Text: decision header for ask/deny
        if decision == "ask":
            click.echo("decision: ask (use `dbastion exec` to execute)")
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
            click.echo(f"\nerror: {cost_diag.message}")
            return
        if dry_run_only:
            return
        # Text: execution result
        if isinstance(exec_result, ExecutionResult):
            click.echo(format_execution_result(exec_result, output_format="text"))
