"""Test safety checks."""

import sqlglot

from dbastion.diagnostics import codes
from dbastion.policy.safety import (
    check_constant_condition,
    check_cross_join_no_condition,
    check_delete_without_where,
    check_multiple_statements,
    check_update_without_where,
)


class TestTokenizerCrash:
    def test_unclosed_comment_does_not_crash(self) -> None:
        """Malformed SQL with unclosed comment must not raise an exception."""
        from dbastion.policy import run_policy
        result = run_policy("SELECT 1; /* broken")
        assert result.blocked

    def test_unclosed_string_does_not_crash(self) -> None:
        from dbastion.policy import run_policy
        result = run_policy("SELECT 'unclosed")
        assert result.blocked

    def test_null_byte_does_not_crash(self) -> None:
        from dbastion.policy import run_policy
        # Should not raise — sqlglot treats it as identifier, DB will reject at execution
        run_policy("SELECT \x00")


class TestMultipleStatements:
    def test_blocks_two_statements(self) -> None:
        diag = check_multiple_statements("SELECT 1; DROP TABLE users")
        assert diag is not None
        assert diag.code == codes.MULTIPLE_STATEMENTS

    def test_allows_single_statement(self) -> None:
        assert check_multiple_statements("SELECT id FROM users") is None

    def test_trailing_semicolon_ok(self) -> None:
        # Trailing semicolon should not trigger multiple-statements error
        assert check_multiple_statements("SELECT id FROM users;") is None


class TestDeleteWithoutWhere:
    def test_blocks_bare_delete(self) -> None:
        stmt = sqlglot.parse_one("DELETE FROM orders")
        diag = check_delete_without_where(stmt, "DELETE FROM orders")
        assert diag is not None
        assert diag.code == codes.DELETE_WITHOUT_WHERE

    def test_allows_delete_with_where(self) -> None:
        stmt = sqlglot.parse_one("DELETE FROM orders WHERE id = 1")
        assert check_delete_without_where(stmt, "DELETE FROM orders WHERE id = 1") is None

    def test_ignores_non_delete(self) -> None:
        stmt = sqlglot.parse_one("SELECT 1")
        assert check_delete_without_where(stmt, "SELECT 1") is None


class TestUpdateWithoutWhere:
    def test_blocks_bare_update(self) -> None:
        stmt = sqlglot.parse_one("UPDATE orders SET status = 'cancelled'")
        diag = check_update_without_where(stmt, "UPDATE orders SET status = 'cancelled'")
        assert diag is not None
        assert diag.code == codes.UPDATE_WITHOUT_WHERE

    def test_allows_update_with_where(self) -> None:
        sql = "UPDATE orders SET status = 'cancelled' WHERE id = 1"
        stmt = sqlglot.parse_one(sql)
        assert check_update_without_where(stmt, sql) is None

    def test_ignores_non_update(self) -> None:
        stmt = sqlglot.parse_one("SELECT 1")
        assert check_update_without_where(stmt, "SELECT 1") is None


class TestCrossJoinNoCondition:
    def test_explicit_cross_join(self) -> None:
        sql = "SELECT * FROM a CROSS JOIN b"
        stmt = sqlglot.parse_one(sql)
        diag = check_cross_join_no_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CROSS_JOIN_NO_CONDITION

    def test_implicit_cross_join_no_where(self) -> None:
        sql = "SELECT * FROM a, b"
        stmt = sqlglot.parse_one(sql)
        diag = check_cross_join_no_condition(stmt, sql)
        assert diag is not None

    def test_implicit_join_with_where_ok(self) -> None:
        sql = "SELECT * FROM a, b WHERE a.id = b.id"
        stmt = sqlglot.parse_one(sql)
        assert check_cross_join_no_condition(stmt, sql) is None

    def test_join_without_on_with_unrelated_where_warns(self) -> None:
        sql = "SELECT * FROM a JOIN b WHERE a.id = 1"
        stmt = sqlglot.parse_one(sql)
        diag = check_cross_join_no_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CROSS_JOIN_NO_CONDITION

    def test_implicit_join_with_unrelated_where_warns(self) -> None:
        sql = "SELECT * FROM a, b WHERE a.id = 1"
        stmt = sqlglot.parse_one(sql)
        diag = check_cross_join_no_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CROSS_JOIN_NO_CONDITION

    def test_join_without_on_with_link_predicate_ok(self) -> None:
        sql = "SELECT * FROM a JOIN b WHERE a.id = b.id"
        stmt = sqlglot.parse_one(sql)
        assert check_cross_join_no_condition(stmt, sql) is None

    def test_alias_link_predicate_ok(self) -> None:
        sql = "SELECT * FROM a AS x JOIN b AS y WHERE x.id = y.id"
        stmt = sqlglot.parse_one(sql)
        assert check_cross_join_no_condition(stmt, sql) is None

    def test_natural_join_ok(self) -> None:
        sql = "SELECT * FROM a NATURAL JOIN b"
        stmt = sqlglot.parse_one(sql)
        assert check_cross_join_no_condition(stmt, sql) is None

    def test_proper_join_ok(self) -> None:
        sql = "SELECT * FROM a JOIN b ON a.id = b.id"
        stmt = sqlglot.parse_one(sql)
        assert check_cross_join_no_condition(stmt, sql) is None

    def test_single_table_ok(self) -> None:
        sql = "SELECT * FROM a"
        stmt = sqlglot.parse_one(sql)
        assert check_cross_join_no_condition(stmt, sql) is None


class TestConstantCondition:
    def test_where_1_eq_1(self) -> None:
        sql = "SELECT * FROM t WHERE 1=1"
        stmt = sqlglot.parse_one(sql)
        diag = check_constant_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CONSTANT_CONDITION

    def test_where_true(self) -> None:
        sql = "SELECT * FROM t WHERE true"
        stmt = sqlglot.parse_one(sql)
        diag = check_constant_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CONSTANT_CONDITION

    def test_nested_or_true(self) -> None:
        sql = "SELECT * FROM t WHERE id = 1 OR TRUE"
        stmt = sqlglot.parse_one(sql)
        diag = check_constant_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CONSTANT_CONDITION

    def test_nested_and_tautology(self) -> None:
        sql = "SELECT * FROM t WHERE 1=1 AND id = 1"
        stmt = sqlglot.parse_one(sql)
        diag = check_constant_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CONSTANT_CONDITION

    def test_literal_string_tautology(self) -> None:
        sql = "SELECT * FROM t WHERE 'x' = 'x'"
        stmt = sqlglot.parse_one(sql)
        diag = check_constant_condition(stmt, sql)
        assert diag is not None
        assert diag.code == codes.CONSTANT_CONDITION

    def test_normal_where_ok(self) -> None:
        sql = "SELECT * FROM t WHERE id = 1"
        stmt = sqlglot.parse_one(sql)
        assert check_constant_condition(stmt, sql) is None

    def test_non_tautology_literal_comparison_ok(self) -> None:
        sql = "SELECT * FROM t WHERE 1 = 2"
        stmt = sqlglot.parse_one(sql)
        assert check_constant_condition(stmt, sql) is None

    def test_no_where_ok(self) -> None:
        sql = "SELECT * FROM t"
        stmt = sqlglot.parse_one(sql)
        assert check_constant_condition(stmt, sql) is None
