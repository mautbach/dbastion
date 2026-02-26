"""Classify SQL statements by type (READ, DML, DDL)."""

from __future__ import annotations

from sqlglot import exp

from dbastion.policy._types import StatementType

_READ_TYPES = (exp.Select, exp.Union, exp.Intersect, exp.Except)
_DML_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
_DDL_TYPES = (exp.Create, exp.Drop, exp.Alter, exp.TruncateTable)

# Statements that are always blocked (privilege escalation, data movement, etc.).
_BLOCKED_TYPES = (exp.Grant, exp.Copy, exp.Command)


def _has_dml_in_cte(statement: exp.Expression) -> bool:
    """Check if any CTE contains a DML operation (writable CTE)."""
    return any(
        isinstance(cte.this, _DML_TYPES)
        for cte in statement.find_all(exp.CTE)
    )


def _has_into(statement: exp.Expression) -> bool:
    """Check for SELECT INTO (creates a table despite being a SELECT)."""
    return isinstance(statement, exp.Select) and statement.find(exp.Into) is not None


def classify(statement: exp.Expression) -> StatementType:
    """Classify a parsed SQL statement.

    Security-critical: anything we can't positively identify as a safe READ
    is classified as DML, DDL, or UNKNOWN (all blocked by default).

    Catches:
    - Writable CTEs: WITH d AS (DELETE ...) SELECT * FROM d
    - SELECT INTO: SELECT * INTO new_table FROM t (creates table)
    - GRANT/COPY/Command: always classified as ADMIN (blocked)
    - TruncateTable: classified as DDL
    - Unknown statements (DO, PREPARE, SET ROLE): UNKNOWN → blocked
    """
    if isinstance(statement, _BLOCKED_TYPES):
        return StatementType.ADMIN
    if isinstance(statement, _READ_TYPES):
        if _has_dml_in_cte(statement):
            return StatementType.DML
        if _has_into(statement):
            return StatementType.DDL
        return StatementType.READ
    if isinstance(statement, _DML_TYPES):
        return StatementType.DML
    if isinstance(statement, _DDL_TYPES):
        return StatementType.DDL
    return StatementType.UNKNOWN
