"""Test enrichment: auto-LIMIT injection."""

import sqlglot

from dbastion.diagnostics import codes
from dbastion.policy.enrich import inject_limit


class TestInjectLimit:
    def test_adds_limit_to_bare_select(self) -> None:
        stmt = sqlglot.parse_one("SELECT id, name FROM users")
        modified, diag = inject_limit(stmt, limit=1000)
        assert diag is not None
        assert diag.code == codes.LIMIT_INJECTED
        sql = modified.sql()
        assert "LIMIT" in sql.upper()

    def test_preserves_existing_limit(self) -> None:
        stmt = sqlglot.parse_one("SELECT id FROM users LIMIT 10")
        modified, diag = inject_limit(stmt, limit=1000)
        assert diag is None
        assert "10" in modified.sql()

    def test_skips_group_by_queries(self) -> None:
        stmt = sqlglot.parse_one("SELECT status, COUNT(*) FROM orders GROUP BY status")
        _, diag = inject_limit(stmt, limit=1000)
        assert diag is None

    def test_custom_limit_value(self) -> None:
        stmt = sqlglot.parse_one("SELECT id FROM users")
        modified, diag = inject_limit(stmt, limit=50)
        assert diag is not None
        assert "50" in modified.sql()

    def test_ignores_non_select(self) -> None:
        stmt = sqlglot.parse_one("INSERT INTO t (a) VALUES (1)")
        _, diag = inject_limit(stmt, limit=1000)
        assert diag is None
