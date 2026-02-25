"""CTE-aware table extraction using sqlglot scope analysis."""

from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope


def extract_tables(statement: exp.Expression) -> list[str]:
    """Extract all referenced table names from a SQL statement.

    Resolves CTEs — only returns real (physical) table names.
    Handles SELECT, JOIN, subqueries, UNION, INSERT, UPDATE, DELETE.

    Returns sorted list of fully-qualified table names (schema.table when schema is present).
    """
    cte_names: set[str] = set()
    source_tables: set[str] = set()

    try:
        scopes = list(traverse_scope(statement))
    except Exception:
        # Fall back to simple AST walk if scope analysis fails
        # (e.g. for DDL statements that don't have scopes)
        return _walk_tables(statement)

    if not scopes:
        return _walk_tables(statement)

    # Pass 1: collect CTE names
    for scope in scopes:
        if scope.is_cte:
            cte_names.add(scope.expression.parent.alias)

    # Pass 2: collect real tables from all scopes
    for scope in scopes:
        for table in scope.tables:
            if table.name not in cte_names:
                source_tables.add(_qualified_name(table))

    # Pass 3: DML targets (INSERT INTO, DELETE FROM, UPDATE) aren't in scopes
    dml_types = (exp.Insert, exp.Delete, exp.Update)
    for node in (statement.find(t) for t in dml_types):
        if node is not None:
            table = node.find(exp.Table)
            if table is not None and table.name not in cte_names:
                source_tables.add(_qualified_name(table))

    return sorted(source_tables)


def _walk_tables(statement: exp.Expression) -> list[str]:
    """Simple AST walk fallback for DDL and other non-scoped statements."""
    tables: set[str] = set()
    for node in statement.walk():
        if isinstance(node, exp.Table) and node.name:
            tables.add(_qualified_name(node))
    return sorted(tables)


def _qualified_name(table: exp.Table) -> str:
    """Build schema.table or just table name."""
    if table.db:
        return f"{table.db}.{table.name}"
    return table.name
