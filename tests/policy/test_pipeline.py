"""Integration tests for the full policy pipeline."""

from dbastion.diagnostics import codes
from dbastion.policy import run_policy


class TestClassificationBlocking:
    def test_select_allowed(self) -> None:
        result = run_policy("SELECT id FROM users")
        assert not result.blocked
        assert result.classification == "read"

    def test_insert_blocked_by_default(self) -> None:
        result = run_policy("INSERT INTO users (name) VALUES ('test')")
        assert result.blocked
        assert any(d.code == codes.WRITE_BLOCKED for d in result.diagnostics)
        assert result.classification == "dml"

    def test_insert_allowed_with_flag(self) -> None:
        result = run_policy("INSERT INTO users (name) VALUES ('test')", allow_write=True)
        assert not result.blocked
        assert result.classification == "dml"

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
        assert result.classification == "ddl"

    def test_drop_blocked(self) -> None:
        result = run_policy("DROP TABLE users")
        assert result.blocked


class TestAdminBlocked:
    def test_grant_always_blocked(self) -> None:
        result = run_policy("GRANT SELECT ON users TO readonly_role", allow_write=True)
        assert result.blocked
        assert any(d.code == codes.ADMIN_BLOCKED for d in result.diagnostics)

    def test_copy_always_blocked(self) -> None:
        result = run_policy("COPY users FROM '/tmp/data.csv'", allow_write=True)
        assert result.blocked
        assert any(d.code == codes.ADMIN_BLOCKED for d in result.diagnostics)


class TestUnclassifiedBlocked:
    def test_unknown_statement_blocked(self) -> None:
        # DO blocks parse as Command in sqlglot
        result = run_policy("DO $$ BEGIN RAISE NOTICE 'hi'; END $$")
        assert result.blocked


class TestWritableCteBlocking:
    def test_delete_in_cte_blocked(self) -> None:
        sql = "WITH d AS (DELETE FROM t WHERE id=1 RETURNING *) SELECT * FROM d"
        result = run_policy(sql)
        assert result.blocked
        assert any(d.code == codes.WRITE_BLOCKED for d in result.diagnostics)

    def test_delete_in_cte_allowed_with_write(self) -> None:
        sql = "WITH d AS (DELETE FROM t WHERE id=1 RETURNING *) SELECT * FROM d"
        result = run_policy(sql, allow_write=True)
        assert not result.blocked

    def test_select_into_blocked(self) -> None:
        result = run_policy("SELECT * INTO new_table FROM users")
        assert result.blocked
        assert any(d.code == codes.DDL_BLOCKED for d in result.diagnostics)

    def test_truncate_blocked(self) -> None:
        result = run_policy("TRUNCATE TABLE users")
        assert result.blocked
        assert any(d.code == codes.DDL_BLOCKED for d in result.diagnostics)


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


class TestDangerousFunctions:
    _PG_BLOCKLIST = frozenset({
        "pg_terminate_backend", "pg_cancel_backend", "pg_read_file",
        "pg_read_binary_file", "lo_import", "lo_export",
        "pg_advisory_lock", "pg_advisory_xact_lock", "set_config",
        "pg_switch_wal", "pg_create_restore_point",
    })

    def test_pg_terminate_backend_blocked(self) -> None:
        result = run_policy(
            "SELECT pg_terminate_backend(pg_backend_pid())",
            dangerous_functions=self._PG_BLOCKLIST,
        )
        assert result.blocked
        assert any(d.code == codes.DANGEROUS_FUNCTION for d in result.diagnostics)

    def test_pg_read_file_blocked(self) -> None:
        result = run_policy(
            "SELECT pg_read_file('/etc/passwd')",
            dangerous_functions=self._PG_BLOCKLIST,
        )
        assert result.blocked

    def test_set_config_blocked(self) -> None:
        result = run_policy(
            "SELECT set_config('log_connections', 'off', false)",
            dangerous_functions=self._PG_BLOCKLIST,
        )
        assert result.blocked

    def test_safe_function_allowed(self) -> None:
        result = run_policy(
            "SELECT now(), version()",
            dangerous_functions=self._PG_BLOCKLIST,
        )
        assert not result.blocked

    def test_no_blocklist_allows_everything(self) -> None:
        result = run_policy("SELECT pg_terminate_backend(123)")
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
