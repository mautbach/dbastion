"""Safety checks: dangerous pattern detection in SQL AST."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from dbastion.diagnostics import Diagnostic, Span, codes


def check_multiple_statements(sql: str) -> Diagnostic | None:
    """Block SQL containing multiple statements (possible injection)."""
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.ParseError:
        return None

    # Filter out empty expressions (trailing semicolons)
    statements = [s for s in statements if s is not None]
    if len(statements) <= 1:
        return None

    semi_pos = sql.find(";")
    if semi_pos == -1:
        semi_pos = len(sql) // 2

    return (
        Diagnostic.error(codes.MULTIPLE_STATEMENTS, "multiple statements detected")
        .span(Span(semi_pos, semi_pos + 1), "second statement starts here")
        .note("only single statements are allowed (possible SQL injection)")
        .note("split into separate dbastion query calls if intentional")
    )


def check_delete_without_where(statement: exp.Expression, sql: str) -> Diagnostic | None:
    """Block DELETE statements that have no WHERE clause."""
    if not isinstance(statement, exp.Delete):
        return None
    if statement.find(exp.Where) is not None:
        return None

    return (
        Diagnostic.error(codes.DELETE_WITHOUT_WHERE, "DELETE without WHERE clause")
        .note("this would affect all rows in the table")
        .suggest_template("add a WHERE clause: DELETE FROM ... WHERE <condition>")
    )


def check_update_without_where(statement: exp.Expression, sql: str) -> Diagnostic | None:
    """Block UPDATE statements that have no WHERE clause."""
    if not isinstance(statement, exp.Update):
        return None
    if statement.find(exp.Where) is not None:
        return None

    return (
        Diagnostic.error(codes.UPDATE_WITHOUT_WHERE, "UPDATE without WHERE clause")
        .note("this would affect all rows in the table")
        .suggest_template("add a WHERE clause: UPDATE ... SET ... WHERE <condition>")
    )
