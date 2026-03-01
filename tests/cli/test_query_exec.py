"""CLI integration tests for query/exec decision behaviour."""

from __future__ import annotations

import json
import tempfile

import duckdb
from click.testing import CliRunner

from dbastion.cli import main


class TestQueryDecision:
    """query command: decision=allow for reads, ask for writes, deny for blocked."""

    def test_read_returns_allow(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"
        assert data["classification"] == "read"
        assert data["columns"] == ["x"]

    def test_dml_returns_ask(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
            db_path = f.name
        # Pre-create table so dry-run succeeds
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO users VALUES (1, 'alice')")
        conn.close()

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "DELETE FROM users WHERE id = 1",
            "--db", f"duckdb:path={db_path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "ask"
        assert data["classification"] == "dml"
        assert "columns" not in data  # not executed

    def test_ddl_returns_ask(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
            db_path = f.name
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE users (id INTEGER)")
        conn.close()

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "DROP TABLE users",
            "--db", f"duckdb:path={db_path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "ask"
        assert data["classification"] == "ddl"

    def test_delete_without_where_returns_deny(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "DELETE FROM users",
            "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert data["classification"] == "dml"
        assert data["blocked"] is True

    def test_admin_returns_deny(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "GRANT SELECT ON t TO public",
            "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert data["classification"] == "admin"

    def test_multiple_statements_returns_deny(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1; DROP TABLE x",
            "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert data["blocked"] is True

    def test_no_allow_write_flag(self) -> None:
        """--allow-write should not exist on query."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--allow-write",
        ])
        assert result.exit_code != 0
        assert "No such option" in result.output or "no such option" in result.output

    def test_dry_run_read_returns_allow(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:",
            "--format", "json", "--dry-run",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"
        assert data.get("dry_run") is True
        assert "columns" not in data  # not executed


class TestExecDecision:
    """exec command: blocks reads, executes writes, denies safety violations."""

    def test_read_blocked(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "exec", "SELECT 1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "write operations" in data["error"]

    def test_delete_without_where_denied(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "exec", "DELETE FROM users",
            "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert data["blocked"] is True

    def test_admin_denied(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "exec", "GRANT ALL ON t TO public",
            "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"

    def test_ddl_executes(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
            db_path = f.name

        runner = CliRunner()
        result = runner.invoke(main, [
            "exec", "CREATE TABLE test_exec_ddl (id INTEGER)",
            "--db", f"duckdb:path={db_path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"
        assert data["classification"] == "ddl"


class TestDDLDryRun:
    """DDL dry-run: DuckDB supports EXPLAIN for DDL, Postgres does not."""

    def test_ddl_with_estimate_returns_ask(self) -> None:
        """DuckDB supports EXPLAIN for DDL — estimate is included."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
            db_path = f.name
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE users (id INTEGER)")
        conn.close()

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "DROP TABLE users",
            "--db", f"duckdb:path={db_path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "ask"
        assert data["classification"] == "ddl"

    def test_ddl_nonexistent_table_denied(self) -> None:
        """DDL on nonexistent table is a real error, not silently swallowed."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "DROP TABLE nonexistent_xyz",
            "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"


class TestCostThresholdNoEstimate:
    """When adapter can't estimate cost and thresholds are set, deny."""

    def test_query_thresholds_without_estimate_denied(self, monkeypatch) -> None:
        """query with --max-gb should deny when dry-run returns None."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:",
            "--format", "json", "--max-gb", "10",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")

    def test_exec_thresholds_without_estimate_denied(self, monkeypatch) -> None:
        """exec with --max-rows should deny when dry-run returns None."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        # Use DML so exec doesn't reject it as a read
        result = runner.invoke(main, [
            "exec", "DELETE FROM t WHERE id=1", "--db", "duckdb:",
            "--format", "json", "--max-rows", "100",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")

    def test_query_no_thresholds_without_estimate_proceeds(self, monkeypatch) -> None:
        """Without thresholds, None estimate is fine — proceed normally."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:", "--format", "json",
            "--max-gb", "0",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"


class TestFromStdin:
    """--from-stdin: read SQL from stdin (query only)."""

    def test_query_from_stdin(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "--from-stdin", "--db", "duckdb:", "--format", "json",
        ], input="SELECT 1 AS x")
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"
        assert data["columns"] == ["x"]

    def test_query_both_sql_and_stdin_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--from-stdin", "--db", "duckdb:",
        ], input="SELECT 2")
        assert result.exit_code != 0
        assert "not both" in result.output

    def test_query_no_sql_no_stdin_rejected(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "--db", "duckdb:",
        ])
        assert result.exit_code != 0
        assert "SQL" in result.output

    def test_exec_no_from_stdin_flag(self) -> None:
        """--from-stdin should not exist on exec (security: SQL must be visible)."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "exec", "--from-stdin", "--db", "duckdb:",
        ], input="SELECT 1")
        assert result.exit_code != 0
        assert "No such option" in result.output or "no such option" in result.output


class TestMissingDriver:
    """Missing adapter extras produce clean errors, not tracebacks."""

    def _simulate_missing_driver(self, monkeypatch) -> None:
        """Point the DuckDB adapter entry at a nonexistent module.

        This triggers the real ImportError → AdapterError path in get_adapter().
        """
        from dbastion.adapters import _registry
        from dbastion.adapters._base import DatabaseType

        patched = dict(_registry._ADAPTER_MAP)
        patched[DatabaseType.DUCKDB] = ("dbastion.adapters._no_such_module", "X")
        monkeypatch.setattr(_registry, "_ADAPTER_MAP", patched)

    def test_query_missing_driver_json(self, monkeypatch) -> None:
        self._simulate_missing_driver(monkeypatch)
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "Missing driver" in data["error"]
        assert "Traceback" not in result.output

    def test_query_missing_driver_text(self, monkeypatch) -> None:
        self._simulate_missing_driver(monkeypatch)
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--format", "text",
        ])
        assert result.exit_code == 1
        assert "Missing driver" in result.output
        assert "Traceback" not in result.output

    def test_exec_missing_driver_json(self, monkeypatch) -> None:
        self._simulate_missing_driver(monkeypatch)
        runner = CliRunner()
        result = runner.invoke(main, [
            "exec", "DELETE FROM t WHERE id=1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "Missing driver" in data["error"]
        assert "Traceback" not in result.output
