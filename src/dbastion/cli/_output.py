"""Shared output formatting for CLI commands."""

from __future__ import annotations

import json

from dbastion.adapters._base import CostEstimate, ExecutionResult
from dbastion.diagnostics.render import render_json, render_text
from dbastion.diagnostics.types import DiagnosticResult


def format_result(result: DiagnosticResult, *, output_format: str = "text") -> str:
    if output_format == "json":
        return json.dumps(render_json(result), indent=2)
    return render_text(result)


def render_estimate(estimate: CostEstimate) -> str:
    """Render a cost estimate for text output."""
    lines = [f"\nestimate: {estimate.summary}"]
    for w in estimate.warnings:
        lines.append(f"  warning: {w}")
    return "\n".join(lines)


def format_execution_result(result: ExecutionResult, *, output_format: str = "text") -> str:
    if output_format == "json":
        data = {
            "columns": result.columns,
            "rows": result.rows,
            "row_count": result.row_count,
            "duration_ms": result.duration_ms,
        }
        if result.cost is not None:
            data["cost"] = {
                "estimated_gb": result.cost.estimated_gb,
                "estimated_cost_usd": result.cost.estimated_cost_usd,
                "summary": result.cost.summary,
            }
        return json.dumps(data, indent=2, default=str)

    # Text format: simple tabular output.
    lines: list[str] = []
    if result.columns:
        lines.append(" | ".join(result.columns))
        lines.append("-+-".join("-" * max(len(c), 5) for c in result.columns))
        for row in result.rows:
            lines.append(" | ".join(str(row.get(c, "")) for c in result.columns))

    duration = f", {result.duration_ms:.0f}ms" if result.duration_ms is not None else ""
    lines.append(f"\n({result.row_count} rows{duration})")
    if result.cost and result.cost.summary:
        lines.append(f"cost: {result.cost.summary}")
    return "\n".join(lines)
