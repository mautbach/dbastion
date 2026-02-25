"""Policy engine: parse, classify, guard, enrich, return diagnostics."""

from __future__ import annotations

import sqlglot

from dbastion.diagnostics import Diagnostic, DiagnosticResult, codes
from dbastion.policy._types import StatementType
from dbastion.policy.classify import classify
from dbastion.policy.enrich import inject_limit
from dbastion.policy.safety import (
    check_delete_without_where,
    check_multiple_statements,
    check_update_without_where,
)
from dbastion.policy.tables import extract_tables


def run_policy(
    sql: str,
    *,
    dialect: str | None = None,
    allow_write: bool = False,
    limit: int | None = 1000,
) -> DiagnosticResult:
    """Run the full policy pipeline on a SQL string.

    Steps:
        1. Check for multiple statements (injection detection)
        2. Parse SQL into AST
        3. Classify (READ/DML/DDL)
        4. Access control (block writes unless allowed)
        5. Safety checks (DELETE/UPDATE without WHERE)
        6. Enrichment (auto-LIMIT)
        7. Return DiagnosticResult

    Args:
        sql: The raw SQL string from the agent.
        dialect: SQL dialect for parsing (None = auto-detect).
        allow_write: If True, DML/DDL statements are allowed.
        limit: Auto-LIMIT value for unbounded SELECTs. None disables.

    Returns:
        DiagnosticResult with all diagnostics and the effective SQL.
    """
    sql = sql.strip()
    diagnostics: list[Diagnostic] = []

    # Step 1: Multiple statement check
    multi_diag = check_multiple_statements(sql)
    if multi_diag is not None:
        diagnostics.append(multi_diag)
        return DiagnosticResult(
            original_sql=sql,
            healed_sql=None,
            diagnostics=diagnostics,
            blocked=True,
        )

    # Step 2: Parse
    try:
        statement = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        diagnostics.append(
            Diagnostic.error(codes.SYNTAX_ERROR, f"SQL syntax error: {e}")
        )
        return DiagnosticResult(
            original_sql=sql,
            healed_sql=None,
            diagnostics=diagnostics,
            blocked=True,
        )

    # Step 3: Extract tables + Classify
    tables = extract_tables(statement)
    stmt_type = classify(statement)

    # Step 4: Access control
    if stmt_type == StatementType.DML and not allow_write:
        diagnostics.append(
            Diagnostic.error(codes.WRITE_BLOCKED, "write operation blocked")
            .note("pass --allow-write to enable DML operations")
        )
    elif stmt_type == StatementType.DDL and not allow_write:
        diagnostics.append(
            Diagnostic.error(codes.DDL_BLOCKED, "DDL operation blocked")
            .note("pass --allow-write to enable DDL operations")
        )

    # Step 5: Safety checks
    for check in [check_delete_without_where, check_update_without_where]:
        diag = check(statement, sql)
        if diag is not None:
            diagnostics.append(diag)

    # Step 6: Enrichment (only if not already blocked and is a read)
    healed_sql = None
    blocked = any(d.is_blocking for d in diagnostics)

    if not blocked and stmt_type == StatementType.READ and limit is not None:
        statement, limit_diag = inject_limit(statement, limit=limit)
        if limit_diag is not None:
            diagnostics.append(limit_diag)
            healed_sql = statement.sql(dialect=dialect)

    blocked = any(d.is_blocking for d in diagnostics)

    return DiagnosticResult(
        original_sql=sql,
        healed_sql=healed_sql,
        diagnostics=diagnostics,
        blocked=blocked,
        tables=tables,
    )
