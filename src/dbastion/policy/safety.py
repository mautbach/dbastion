"""Safety checks: dangerous pattern detection in SQL AST."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from dbastion.diagnostics import Diagnostic, Span, codes


def check_multiple_statements(sql: str) -> Diagnostic | None:
    """Block SQL containing multiple statements (possible injection)."""
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.SqlglotError:
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


def check_dangerous_functions(
    statement: exp.Expression,
    sql: str,
    blocked_functions: frozenset[str],
) -> Diagnostic | None:
    """Block calls to dangerous database-specific system functions."""
    if not blocked_functions:
        return None

    for func in statement.find_all(exp.Anonymous, exp.Func):
        name = ""
        if isinstance(func, exp.Anonymous):
            name = func.name
        elif hasattr(func, "sql_name"):
            name = func.sql_name()
        elif hasattr(func, "key"):
            name = func.key

        if name.lower() in blocked_functions:
            return Diagnostic.error(
                codes.DANGEROUS_FUNCTION,
                f"dangerous function blocked: {name}",
            ).note("this function can cause damage even inside a SELECT")
    return None


_JOIN_PREDICATE_TYPES = (
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Is,
)


def _relation_identifiers(relation: exp.Expression) -> set[str]:
    """Collect identifiers that can qualify columns for a relation."""
    ids: set[str] = set()

    alias_or_name = getattr(relation, "alias_or_name", None)
    if alias_or_name:
        ids.add(alias_or_name.lower())

    if isinstance(relation, exp.Table) and relation.name:
        ids.add(relation.name.lower())

    return ids


def _table_refs(node: exp.Expression) -> set[str]:
    """Collect qualified table names referenced by columns in an expression."""
    return {c.table.lower() for c in node.find_all(exp.Column) if c.table}


def _where_links_tables(
    where_expr: exp.Where | None,
    *,
    left_ids: set[str],
    right_ids: set[str],
) -> bool:
    """Return True if WHERE contains a predicate linking left and right relations."""
    if where_expr is None:
        return False

    for predicate in where_expr.find_all(*_JOIN_PREDICATE_TYPES):
        refs = _table_refs(predicate)
        if refs & left_ids and refs & right_ids:
            return True
    return False


def check_cross_join_no_condition(statement: exp.Expression, sql: str) -> Diagnostic | None:
    """Warn on CROSS JOINs or joins without any condition (cartesian product)."""
    for select in statement.find_all(exp.Select):
        from_clause = select.args.get("from_")
        if from_clause is None or from_clause.this is None:
            continue

        where_expr = select.args.get("where")
        left_ids = _relation_identifiers(from_clause.this)

        for join in select.args.get("joins") or []:
            right_ids = _relation_identifiers(join.this)
            is_explicit_cross = (
                str(join.args.get("kind") or "").upper() == "CROSS"
                or str(join.args.get("side") or "").upper() == "CROSS"
            )
            has_on_using = (
                join.args.get("on") is not None
                or join.args.get("using") is not None
                or str(join.args.get("method") or "").upper() == "NATURAL"
            )

            # Explicit CROSS JOIN — still allow if WHERE links the tables
            # (BigQuery parses `FROM a, b` as CROSS JOIN even with a linking WHERE).
            if is_explicit_cross and not _where_links_tables(
                where_expr, left_ids=left_ids, right_ids=right_ids,
            ):
                return Diagnostic.warning(
                    codes.CROSS_JOIN_NO_CONDITION,
                    "CROSS JOIN or join without condition produces a cartesian product",
                ).note("this may return an extremely large result set")

            # Implicit join is only allowed if WHERE contains a relation-link predicate.
            if not is_explicit_cross and not has_on_using and not _where_links_tables(
                where_expr, left_ids=left_ids, right_ids=right_ids,
            ):
                return Diagnostic.warning(
                    codes.CROSS_JOIN_NO_CONDITION,
                    "join without condition produces a cartesian product",
                ).note("add ON/USING, or a WHERE predicate linking both tables")

            left_ids |= right_ids
    return None


def check_constant_condition(statement: exp.Expression, sql: str) -> Diagnostic | None:
    """Warn on constant WHERE conditions like WHERE 1=1 or WHERE true."""
    where = statement.find(exp.Where)
    if where is None:
        return None
    condition = where.this

    for node in condition.find_all(exp.Boolean, exp.EQ):
        if isinstance(node, exp.Boolean) and node.this is True:
            return Diagnostic.warning(
                codes.CONSTANT_CONDITION,
                f"constant WHERE condition: {node.sql()}",
            ).note("possible SQL injection pattern or accidental tautology")

        if isinstance(node, exp.EQ):
            left = node.left
            right = node.right
            if (
                isinstance(left, exp.Literal)
                and isinstance(right, exp.Literal)
                and left.is_string == right.is_string
                and left.this == right.this
            ):
                return Diagnostic.warning(
                    codes.CONSTANT_CONDITION,
                    f"constant WHERE condition: {node.sql()}",
                ).note("possible SQL injection pattern or accidental tautology")
    return None


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
