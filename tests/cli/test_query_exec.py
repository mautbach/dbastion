"""CLI integration tests for query decision behaviour."""

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

    def test_no_skip_dry_run_flag(self) -> None:
        """--skip-dry-run should not exist on query (removed for safety)."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--skip-dry-run",
        ])
        assert result.exit_code != 0
        assert "No such option" in result.output or "no such option" in result.output

    def test_no_max_gb_flag(self) -> None:
        """--max-gb should not exist on query (config-only)."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--max-gb", "10",
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

    def test_exec_command_removed(self) -> None:
        """exec command should not exist (merged into query + approve)."""
        runner = CliRunner()
        result = runner.invoke(main, ["exec", "SELECT 1", "--db", "duckdb:"])
        assert result.exit_code != 0
        assert "No such command" in result.output or "Error" in result.output


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


class TestCostThresholdAsk:
    """Cost threshold exceeded → decision: ask (not deny). Human can approve."""

    def test_cost_exceeded_returns_ask(self, monkeypatch) -> None:
        from dbastion.adapters import duckdb as duckdb_adapter
        from dbastion.adapters._base import CostEstimate, CostUnit

        async def _dry_run_big(self, sql):
            return CostEstimate(
                raw_value=100e9, unit=CostUnit.BYTES,
                estimated_gb=200, summary="200 GB",
            )

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_big)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["decision"] == "ask"
        assert "200.0 GB" in data.get("cost_error", "")
        assert data.get("approval_hint") is not None

    def test_cost_within_default_threshold_allows(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"

    def test_no_estimate_proceeds(self, monkeypatch) -> None:
        """When adapter can't estimate, proceed normally (best-effort)."""
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


class TestThresholdConnectionConfig:
    """Per-connection cost thresholds from connections.toml."""

    def test_connection_max_gb_triggers_ask(self, monkeypatch, tmp_path) -> None:
        """max_gb in connection config triggers ask when exceeded."""
        from dbastion import connections
        from dbastion.adapters import duckdb as duckdb_adapter
        from dbastion.adapters._base import CostEstimate, CostUnit

        async def _dry_run_big(self, sql):
            return CostEstimate(
                raw_value=100e9, unit=CostUnit.BYTES,
                estimated_gb=200, summary="200 GB",
            )

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_big)

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
        assert data["decision"] == "ask"

    def test_connection_max_gb_allows_within(self, monkeypatch, tmp_path) -> None:
        """max_gb in connection config allows when within threshold."""
        from dbastion import connections

        toml_file = tmp_path / "connections.toml"
        toml_file.write_text(
            '[testconn]\ntype = "duckdb"\nmax_gb = "100"\n'
        )
        monkeypatch.setattr(connections, "_CONNECTIONS_FILE", toml_file)

        runner = CliRunner()
        result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "testconn", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["decision"] == "allow"

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


class TestMissingDriver:
    """Missing adapter extras produce clean errors, not tracebacks."""

    def _simulate_missing_driver(self, monkeypatch) -> None:
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
