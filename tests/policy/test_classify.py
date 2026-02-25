"""Test SQL statement classification."""

import pytest
import sqlglot

from dbastion.policy._types import StatementType
from dbastion.policy.classify import classify


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT id FROM users", StatementType.READ),
        ("SELECT * FROM orders WHERE status = 'active'", StatementType.READ),
        ("SELECT 1", StatementType.READ),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", StatementType.READ),
        ("SELECT a FROM t1 UNION SELECT b FROM t2", StatementType.READ),
        ("INSERT INTO users (name) VALUES ('test')", StatementType.DML),
        ("UPDATE users SET name = 'x' WHERE id = 1", StatementType.DML),
        ("DELETE FROM orders WHERE id = 1", StatementType.DML),
        ("CREATE TABLE test (id INT)", StatementType.DDL),
        ("DROP TABLE test", StatementType.DDL),
        ("ALTER TABLE test ADD COLUMN name TEXT", StatementType.DDL),
    ],
)
def test_classify(sql: str, expected: StatementType) -> None:
    stmt = sqlglot.parse_one(sql)
    assert classify(stmt) == expected
