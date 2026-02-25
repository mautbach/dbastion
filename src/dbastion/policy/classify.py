"""Classify SQL statements by type (READ, DML, DDL)."""

from __future__ import annotations

from sqlglot import exp

from dbastion.policy._types import StatementType

_READ_TYPES = (exp.Select, exp.Union, exp.Intersect, exp.Except)
_DML_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
_DDL_TYPES = (exp.Create, exp.Drop, exp.Alter)


def classify(statement: exp.Expression) -> StatementType:
    """Classify a parsed SQL statement."""
    if isinstance(statement, _READ_TYPES):
        return StatementType.READ
    if isinstance(statement, _DML_TYPES):
        return StatementType.DML
    if isinstance(statement, _DDL_TYPES):
        return StatementType.DDL
    return StatementType.UNKNOWN
