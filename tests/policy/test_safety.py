"""Test safety checks."""

import sqlglot

from dbastion.diagnostics import codes
from dbastion.policy.safety import (
    check_delete_without_where,
    check_multiple_statements,
    check_update_without_where,
)


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
