"""Cost threshold checking — bridge between CostEstimate and diagnostics."""

from __future__ import annotations

from dbastion.adapters._base import CostEstimate
from dbastion.diagnostics import Diagnostic, codes


def check_cost_threshold(
    estimate: CostEstimate,
    *,
    max_gb: float | None = None,
    max_usd: float | None = None,
    max_rows: float | None = None,
) -> Diagnostic | None:
    """Return a Q0401 error diagnostic if the estimate exceeds a threshold.

    Checks in order: GB → USD → rows → plan warnings.
    """
    # BigQuery: byte/cost gating
    if (
        max_gb is not None
        and estimate.estimated_gb is not None
        and estimate.estimated_gb > max_gb
    ):
        diag = Diagnostic.error(
            codes.COST_OVER_THRESHOLD,
            f"query would scan {estimate.estimated_gb:.1f} GB (limit: {max_gb:.1f} GB)",
        )
        if estimate.estimated_cost_usd is not None:
            diag.note(f"estimated cost: ${estimate.estimated_cost_usd:.2f}")
        return diag

    if (
        max_usd is not None
        and estimate.estimated_cost_usd is not None
        and estimate.estimated_cost_usd > max_usd
    ):
        return Diagnostic.error(
            codes.COST_OVER_THRESHOLD,
            f"query cost ${estimate.estimated_cost_usd:.2f} exceeds limit ${max_usd:.2f}",
        )

    # PostgreSQL/general: row estimate gating
    if (
        max_rows is not None
        and estimate.estimated_rows is not None
        and estimate.estimated_rows > max_rows
    ):
        diag = Diagnostic.error(
            codes.COST_OVER_THRESHOLD,
            f"query estimates ~{estimate.estimated_rows:,.0f} rows (limit: {max_rows:,.0f})",
        )
        if estimate.warnings:
            for w in estimate.warnings:
                diag.note(w)
        return diag

    return None
