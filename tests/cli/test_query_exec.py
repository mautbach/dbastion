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

    def test_query_explicit_max_gb_without_estimate_denied(self, monkeypatch) -> None:
        """Explicit --max-gb is strict: deny when dry-run can't estimate."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:",
            "--format", "json", "--max-gb", "10",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")

    def test_query_explicit_threshold_without_estimate_denied(self, monkeypatch) -> None:
        """--max-usd/--max-rows are explicit: deny when dry-run returns None."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:",
            "--format", "json", "--max-usd", "1",
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

    def test_query_default_thresholds_without_estimate_proceeds(self, monkeypatch) -> None:
        """Default max-gb is best-effort, so None estimate proceeds normally."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"


class TestThresholdEnvVars:
    """Environment variables for cost thresholds."""

    def test_max_gb_from_env(self, monkeypatch) -> None:
        """DBASTION_MAX_GB sets --max-gb."""
        from dbastion.adapters import duckdb as duckdb_adapter
        from dbastion.adapters._base import CostEstimate, CostUnit

        async def _dry_run_big(self, sql):
            return CostEstimate(
                raw_value=100e9, unit=CostUnit.BYTES,
                estimated_gb=100, summary="100 GB",
            )

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_big)
        monkeypatch.setenv("DBASTION_MAX_GB", "50")

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "100.0 GB" in data.get("cost_error", "")

    def test_max_gb_cli_overrides_env(self, monkeypatch) -> None:
        """CLI --max-gb takes precedence over DBASTION_MAX_GB."""
        monkeypatch.setenv("DBASTION_MAX_GB", "1")

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:",
            "--format", "json", "--max-gb", "0",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"

    def test_max_gb_env_without_estimate_denied(self, monkeypatch) -> None:
        """DBASTION_MAX_GB is explicit: deny when dry-run can't estimate."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)
        monkeypatch.setenv("DBASTION_MAX_GB", "50")

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")

    def test_max_usd_zero_disables(self, monkeypatch) -> None:
        """--max-usd 0 disables the threshold (same as --max-gb 0)."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:",
            "--format", "json", "--max-usd", "0", "--max-gb", "0",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"

    def test_max_rows_zero_disables(self, monkeypatch) -> None:
        """--max-rows 0 disables the threshold."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:",
            "--format", "json", "--max-rows", "0", "--max-gb", "0",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"

    def test_max_usd_from_env(self, monkeypatch) -> None:
        """DBASTION_MAX_USD sets --max-usd."""
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)
        monkeypatch.setenv("DBASTION_MAX_USD", "10")
        # Also disable max-gb default so it doesn't interfere
        monkeypatch.setenv("DBASTION_MAX_GB", "0")

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")


class TestThresholdConnectionConfig:
    """Per-connection cost thresholds from connections.toml."""

    def test_connection_max_gb(self, monkeypatch, tmp_path) -> None:
        """max_gb in connection config is used as threshold."""
        from dbastion import connections
        from dbastion.adapters import duckdb as duckdb_adapter
        from dbastion.adapters._base import CostEstimate, CostUnit

        async def _dry_run_big(self, sql):
            return CostEstimate(
                raw_value=100e9, unit=CostUnit.BYTES,
                estimated_gb=100, summary="100 GB",
            )

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_big)

        # Create a temp connections file with max_gb
        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "50"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"

    def test_connection_max_gb_without_estimate_denied(self, monkeypatch, tmp_path) -> None:
        """max_gb in connection config is explicit: deny when can't estimate."""
        from dbastion import connections
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "50"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")

    def test_cli_overrides_connection(self, monkeypatch, tmp_path) -> None:
        """CLI --max-gb overrides connection config."""
        from dbastion import connections

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "1"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "testconn",
            "--format", "json", "--max-gb", "0",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"

    def test_connection_max_usd(self, monkeypatch, tmp_path) -> None:
        """max_usd in connection config triggers deny when can't estimate."""
        from dbastion import connections
        from dbastion.adapters import duckdb as duckdb_adapter

        async def _dry_run_none(self, sql):
            return None

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_none)

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "0"\nmax_usd = "10"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "deny"
        assert "cannot estimate" in data.get("cost_error", "")

    def test_invalid_type_fails(self, monkeypatch, tmp_path) -> None:
        """Invalid db type in connections.toml produces a clear error."""
        from dbastion import connections

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "mongodb"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 1
        assert "invalid type" in result.output
        assert "mongodb" in result.output

    def test_missing_type_fails(self, monkeypatch, tmp_path) -> None:
        """Missing type field in connections.toml produces a clear error."""
        from dbastion import connections

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ndsn = "postgresql://localhost/db"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 1
        assert "missing" in result.output.lower()
        assert "type" in result.output

    def test_malformed_threshold_fails(self, monkeypatch, tmp_path) -> None:
        """Invalid threshold values in connections.toml produce a clear error."""
        from dbastion import connections

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "not_a_number"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 1
        assert "Invalid value" in result.output
        assert "max_gb" in result.output

    def test_connection_thresholds_not_in_adapter_params(self, monkeypatch, tmp_path) -> None:
        """max_gb/max_usd/max_rows should not leak into adapter params."""
        from dbastion import connections

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "50"\nmax_usd = "10"\nmax_rows = "1000"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        config = connections.get_connection("testconn")
        assert config is not None
        assert "max_gb" not in config.params
        assert "max_usd" not in config.params
        assert "max_rows" not in config.params
        assert config.max_gb == 50.0
        assert config.max_usd == 10.0
        assert config.max_rows == 1000.0


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
