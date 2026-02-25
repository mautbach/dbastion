"""Query enrichment: auto-LIMIT injection and other MachineApplicable transforms."""

from __future__ import annotations

from sqlglot import exp

from dbastion.diagnostics import Diagnostic, codes

DEFAULT_LIMIT = 1000


def inject_limit(
    statement: exp.Expression,
    *,
    limit: int = DEFAULT_LIMIT,
) -> tuple[exp.Expression, Diagnostic | None]:
    """Add LIMIT to unbounded SELECT statements.

    Returns the (possibly modified) statement and an optional info diagnostic.
    Does NOT add LIMIT if:
    - Statement is not a SELECT
    - LIMIT already present
    - GROUP BY is present (aggregations naturally limit rows)
    """
    if not isinstance(statement, exp.Select):
        return statement, None
    if statement.find(exp.Limit) is not None:
        return statement, None
    if statement.find(exp.Group) is not None:
        return statement, None

    modified = statement.limit(limit)
    diag = (
        Diagnostic.info(codes.LIMIT_INJECTED, f"LIMIT {limit} added to unbounded SELECT")
        .note("override with --no-limit or --limit N")
    )
    return modified, diag
