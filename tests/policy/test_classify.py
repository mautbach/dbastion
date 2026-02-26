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
        # Writable CTEs — DML hidden inside SELECT
        (
            "WITH d AS (DELETE FROM t WHERE id=1 RETURNING *) SELECT * FROM d",
            StatementType.DML,
        ),
        (
            "WITH i AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM i",
            StatementType.DML,
        ),
        (
            "WITH u AS (UPDATE t SET x=1 WHERE id=1 RETURNING *) SELECT * FROM u",
            StatementType.DML,
        ),
        # Mixed: one read CTE + one DML CTE → still DML
        (
            "WITH a AS (SELECT 1), b AS (DELETE FROM t RETURNING *) SELECT * FROM a, b",
            StatementType.DML,
        ),
        # TRUNCATE → DDL
        ("TRUNCATE TABLE users", StatementType.DDL),
        # SELECT INTO → DDL (creates a table)
        ("SELECT * INTO new_table FROM users", StatementType.DDL),
        # GRANT → ADMIN
        ("GRANT SELECT ON users TO readonly_role", StatementType.ADMIN),
        # COPY → ADMIN
        ("COPY users FROM '/tmp/data.csv'", StatementType.ADMIN),
        # INTERSECT / EXCEPT → READ
        (
            "SELECT id FROM t1 INTERSECT SELECT id FROM t2",
            StatementType.READ,
        ),
        (
            "SELECT id FROM t1 EXCEPT SELECT id FROM t2",
            StatementType.READ,
        ),
        # MERGE → DML
        (
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.x = s.x",
            StatementType.DML,
        ),
    ],
)
def test_classify(sql: str, expected: StatementType) -> None:
    stmt = sqlglot.parse_one(sql)
    assert classify(stmt) == expected
