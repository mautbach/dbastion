"""Integration tests for the full policy pipeline."""

from dbastion.diagnostics import codes
from dbastion.policy import run_policy


class TestClassificationBlocking:
    def test_select_allowed(self) -> None:
        result = run_policy("SELECT id FROM users")
        assert not result.blocked

    def test_insert_blocked_by_default(self) -> None:
        result = run_policy("INSERT INTO users (name) VALUES ('test')")
        assert result.blocked
        assert any(d.code == codes.WRITE_BLOCKED for d in result.diagnostics)

    def test_insert_allowed_with_flag(self) -> None:
        result = run_policy("INSERT INTO users (name) VALUES ('test')", allow_write=True)
        assert not result.blocked

    def test_update_blocked_by_default(self) -> None:
        result = run_policy("UPDATE users SET name = 'test' WHERE id = 1")
        assert result.blocked

    def test_delete_blocked_by_default(self) -> None:
        result = run_policy("DELETE FROM users WHERE id = 1")
        assert result.blocked

    def test_ddl_blocked_by_default(self) -> None:
        result = run_policy("CREATE TABLE test (id INT)")
        assert result.blocked
        assert any(d.code == codes.DDL_BLOCKED for d in result.diagnostics)

    def test_drop_blocked(self) -> None:
        result = run_policy("DROP TABLE users")
        assert result.blocked


class TestSafetyInPipeline:
    def test_delete_without_where(self) -> None:
        result = run_policy("DELETE FROM orders", allow_write=True)
        assert result.blocked
        assert any(d.code == codes.DELETE_WITHOUT_WHERE for d in result.diagnostics)

    def test_delete_with_where_allowed(self) -> None:
        result = run_policy("DELETE FROM orders WHERE id = 1", allow_write=True)
        assert not result.blocked

    def test_update_without_where(self) -> None:
        result = run_policy("UPDATE orders SET status = 'x'", allow_write=True)
        assert result.blocked
        assert any(d.code == codes.UPDATE_WITHOUT_WHERE for d in result.diagnostics)

    def test_update_with_where_allowed(self) -> None:
        result = run_policy("UPDATE orders SET status = 'x' WHERE id = 1", allow_write=True)
        assert not result.blocked


class TestMultipleStatements:
    def test_blocked(self) -> None:
        result = run_policy("SELECT 1; DROP TABLE users")
        assert result.blocked
        assert any(d.code == codes.MULTIPLE_STATEMENTS for d in result.diagnostics)


class TestEnrichment:
    def test_auto_limit_injected(self) -> None:
        result = run_policy("SELECT id, name FROM users")
        assert not result.blocked
        assert result.healed_sql is not None
        assert "LIMIT" in result.healed_sql.upper()
        assert any(d.code == codes.LIMIT_INJECTED for d in result.diagnostics)

    def test_existing_limit_preserved(self) -> None:
        result = run_policy("SELECT id FROM users LIMIT 10")
        assert not any(d.code == codes.LIMIT_INJECTED for d in result.diagnostics)
        assert result.healed_sql is None

    def test_group_by_no_limit(self) -> None:
        result = run_policy("SELECT status, COUNT(*) FROM orders GROUP BY status")
        assert not any(d.code == codes.LIMIT_INJECTED for d in result.diagnostics)

    def test_no_limit_config(self) -> None:
        result = run_policy("SELECT id FROM users", limit=None)
        assert not any(d.code == codes.LIMIT_INJECTED for d in result.diagnostics)
        assert result.healed_sql is None

    def test_effective_sql_with_limit(self) -> None:
        result = run_policy("SELECT id FROM users")
        assert "LIMIT" in result.effective_sql.upper()

    def test_effective_sql_already_limited(self) -> None:
        result = run_policy("SELECT id FROM users LIMIT 5")
        assert result.effective_sql == result.original_sql


class TestParseErrors:
    def test_invalid_sql(self) -> None:
        result = run_policy("SELECT FROM")
        assert result.blocked
        assert any(d.code == codes.SYNTAX_ERROR for d in result.diagnostics)

    def test_empty_sql(self) -> None:
        result = run_policy("")
        assert result.blocked
