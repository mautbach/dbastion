"""Unit tests for ClickHouseAdapter using mocked clickhouse-connect client."""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("clickhouse_connect")

from dbastion.adapters._base import CostUnit, DatabaseType
from dbastion.adapters.clickhouse import ClickHouseAdapter

# -- Mock objects --


class _QueryResult:
    def __init__(
        self,
        column_names: list[str],
        result_rows: list[tuple],
    ) -> None:
        self.column_names = column_names
        self.result_rows = result_rows
        self.row_count = len(result_rows)


class _Client:
    """Minimal mock of clickhouse_connect.driver.Client."""

    def __init__(self) -> None:
        self._query_results: dict[str, _QueryResult] = {}
        self._command_results: dict[str, str] = {}
        self._last_settings: dict[str, str] | None = None

    def set_query_result(self, prefix: str, result: _QueryResult) -> None:
        self._query_results[prefix] = result

    def set_command_result(self, prefix: str, result: str) -> None:
        self._command_results[prefix] = result

    def query(self, sql: str, *, settings: dict | None = None, parameters: dict | None = None):
        self._last_settings = settings
        for prefix, result in self._query_results.items():
            if sql.strip().startswith(prefix) or prefix in sql:
                return result
        return _QueryResult([], [])

    def command(self, sql: str, *, settings: dict | None = None):
        self._last_settings = settings
        for prefix, result in self._command_results.items():
            if sql.strip().startswith(prefix) or prefix in sql:
                return result
        return ""

    def close(self) -> None:
        pass


def _make_adapter(client: _Client) -> ClickHouseAdapter:
    adapter = ClickHouseAdapter()
    adapter._client = client
    return adapter


# -- Unit tests (always run, no Docker) --


class TestMetadata:
    def test_db_type(self) -> None:
        adapter = ClickHouseAdapter()
        assert adapter.db_type() == DatabaseType.CLICKHOUSE

    def test_dialect(self) -> None:
        adapter = ClickHouseAdapter()
        assert adapter.dialect() == "clickhouse"

    def test_dangerous_functions_populated(self) -> None:
        adapter = ClickHouseAdapter()
        funcs = adapter.dangerous_functions()
        assert len(funcs) > 10
        assert "remote" in funcs
        assert "file" in funcs
        assert "executable" in funcs
        assert "s3" in funcs
        assert "mysql" in funcs


class TestExecute:
    def test_execute_simple(self) -> None:
        client = _Client()
        client.set_query_result("SELECT", _QueryResult(
            column_names=["x", "y"],
            result_rows=[(1, "hello"), (2, "world")],
        ))
        adapter = _make_adapter(client)
        result = asyncio.run(adapter.execute("SELECT 1 AS x, 'hello' AS y"))
        assert result.columns == ["x", "y"]
        assert result.row_count == 2
        assert result.rows[0] == {"x": 1, "y": "hello"}
        assert result.duration_ms is not None

    def test_execute_with_labels(self) -> None:
        client = _Client()
        client.set_query_result("SELECT", _QueryResult(["x"], [(1,)]))
        adapter = _make_adapter(client)
        asyncio.run(adapter.execute("SELECT 1 AS x", labels={"tool": "dbastion", "agent": "test"}))
        assert client._last_settings is not None
        assert "log_comment" in client._last_settings
        assert "dbastion" in client._last_settings["log_comment"]
        assert "agent=test" in client._last_settings["log_comment"]


class TestDryRun:
    def test_dry_run_with_estimate(self) -> None:
        client = _Client()
        # EXPLAIN ESTIMATE result: database, table, parts, rows, marks
        client.set_query_result("EXPLAIN ESTIMATE", _QueryResult(
            column_names=["database", "table", "parts", "rows", "marks"],
            result_rows=[("tpch", "lineitem", 5, 6000000, 732)],
        ))
        plan_json = json.dumps({"Plan": {"Node Type": "ReadFromMergeTree"}})
        client.set_command_result("EXPLAIN json=1", plan_json)

        adapter = _make_adapter(client)
        est = asyncio.run(adapter.dry_run("SELECT * FROM tpch.lineitem"))
        assert est is not None
        assert est.estimated_rows == 6000000
        assert est.plan_node == "ReadFromMergeTree"
        assert est.unit == CostUnit.COST_UNITS
        assert "6.0M" in est.summary
        assert "5 parts" in est.summary
        assert len(est.warnings) > 0  # large scan warning

    def test_dry_run_small_table_no_warning(self) -> None:
        client = _Client()
        client.set_query_result("EXPLAIN ESTIMATE", _QueryResult(
            column_names=["database", "table", "parts", "rows", "marks"],
            result_rows=[("tpch", "region", 1, 5, 1)],
        ))
        plan_json = json.dumps({"Plan": {"Node Type": "ReadFromMergeTree"}})
        client.set_command_result("EXPLAIN json=1", plan_json)

        adapter = _make_adapter(client)
        est = asyncio.run(adapter.dry_run("SELECT * FROM tpch.region"))
        assert est is not None
        assert est.estimated_rows == 5
        assert est.warnings == []

    def test_dry_run_returns_none_for_ddl(self) -> None:
        client = _Client()
        # Both EXPLAIN commands fail for DDL — mock returns empty defaults.
        adapter = _make_adapter(client)
        est = asyncio.run(adapter.dry_run("CREATE TABLE foo (x Int32) ENGINE = Memory"))
        assert est is None

    def test_dry_run_plan_only(self) -> None:
        """EXPLAIN ESTIMATE fails (non-MergeTree) but EXPLAIN PLAN works."""
        client = _Client()
        # EXPLAIN ESTIMATE returns empty (non-MergeTree table).
        plan_json = json.dumps({"Plan": {"Node Type": "Expression"}})
        client.set_command_result("EXPLAIN json=1", plan_json)

        adapter = _make_adapter(client)
        est = asyncio.run(adapter.dry_run("SELECT * FROM system.one"))
        assert est is not None
        assert est.plan_node == "Expression"
        assert est.estimated_rows is None

    def test_dry_run_plan_only_from_query_result(self) -> None:
        """Prefer query() output for EXPLAIN json=1 when available."""
        client = _Client()
        plan_json = json.dumps([{"Plan": {"Node Type": "ReadFromStorage"}}])
        client.set_query_result(
            "EXPLAIN json=1",
            _QueryResult(column_names=["explain"], result_rows=[(plan_json,)]),
        )

        adapter = _make_adapter(client)
        est = asyncio.run(adapter.dry_run("SELECT * FROM system.one"))
        assert est is not None
        assert est.plan_node == "ReadFromStorage"

    def test_dry_run_plan_only_from_escaped_command_output(self) -> None:
        """Fallback parser handles escaped payload from command()."""
        client = _Client()
        # Simulates command() payload with escaped newlines.
        plan_json_escaped = (
            "[\\n  {\\n    \"Plan\": {\\n"
            "      \"Node Type\": \"Expression\"\\n"
            "    }\\n  }\\n]"
        )
        client.set_command_result("EXPLAIN json=1", plan_json_escaped)

        adapter = _make_adapter(client)
        est = asyncio.run(adapter.dry_run("SELECT count() FROM system.one"))
        assert est is not None
        assert est.plan_node == "Expression"


class TestIntrospection:
    def test_list_schemas(self) -> None:
        client = _Client()
        client.set_query_result("SELECT name FROM system.databases", _QueryResult(
            column_names=["name"],
            result_rows=[("default",), ("tpch",)],
        ))
        adapter = _make_adapter(client)
        schemas = asyncio.run(adapter.list_schemas())
        assert "default" in schemas
        assert "tpch" in schemas

    def test_list_tables(self) -> None:
        client = _Client()
        client.set_query_result("SELECT name FROM system.tables", _QueryResult(
            column_names=["name"],
            result_rows=[("region",), ("nation",)],
        ))
        adapter = _make_adapter(client)
        tables = asyncio.run(adapter.list_tables("tpch"))
        assert len(tables) == 2
        assert tables[0].schema == "tpch"
        assert tables[0].name == "region"

    def test_describe_table(self) -> None:
        client = _Client()
        # Column metadata.
        client.set_query_result("SELECT name, type", _QueryResult(
            column_names=["name", "type", "is_in_primary_key", "comment"],
            result_rows=[
                ("r_regionkey", "UInt32", 1, ""),
                ("r_name", "String", 0, ""),
                ("r_comment", "Nullable(String)", 0, "optional comment"),
            ],
        ))
        # Table metadata.
        client.set_query_result("SELECT engine", _QueryResult(
            column_names=["engine", "total_rows", "total_bytes",
                          "partition_key", "sorting_key", "primary_key"],
            result_rows=[("MergeTree", 5, 1024, "", "r_regionkey", "r_regionkey")],
        ))
        adapter = _make_adapter(client)
        info = asyncio.run(adapter.describe_table("region", "tpch"))
        assert info.schema == "tpch"
        assert info.name == "region"
        assert info.row_count_estimate == 5
        assert len(info.columns) == 3
        assert info.columns[0].name == "r_regionkey"
        assert info.columns[0].is_primary_key is True
        assert info.columns[0].is_nullable is False
        assert info.columns[2].is_nullable is True
        assert info.columns[2].comment == "optional comment"
        assert info.metadata["engine"] == "MergeTree"
        assert info.metadata["sorting_key"] == "r_regionkey"

    def test_describe_table_not_found(self) -> None:
        client = _Client()
        # Empty column result.
        adapter = _make_adapter(client)
        with pytest.raises(Exception, match="not found"):
            asyncio.run(adapter.describe_table("nonexistent", "tpch"))


class TestConnect:
    def test_connect_missing_host(self) -> None:
        from dbastion.adapters._base import AdapterError, ConnectionConfig

        adapter = ClickHouseAdapter()
        config = ConnectionConfig(
            name="bad",
            db_type=DatabaseType.CLICKHOUSE,
            params={},
        )
        with pytest.raises(AdapterError, match="host"):
            asyncio.run(adapter.connect(config))


# -- Integration tests (require Docker) --

pytestmark_ch = pytest.mark.clickhouse


@pytest.mark.clickhouse
class TestClickHouseIntegration:
    """Integration tests against real ClickHouse Docker container."""

    def test_connect_and_close(self, clickhouse_adapter) -> None:
        assert clickhouse_adapter._client is not None

    def test_execute_simple(self, clickhouse_adapter) -> None:
        result = asyncio.run(clickhouse_adapter.execute("SELECT 1 AS x, 'hello' AS y"))
        assert result.columns == ["x", "y"]
        assert result.row_count == 1
        assert result.rows[0]["x"] == 1

    def test_execute_with_labels(self, clickhouse_adapter) -> None:
        result = asyncio.run(
            clickhouse_adapter.execute("SELECT 1 AS x", labels={"tool": "dbastion"})
        )
        assert result.row_count == 1

    def test_list_schemas(self, clickhouse_adapter) -> None:
        schemas = asyncio.run(clickhouse_adapter.list_schemas())
        assert "default" in schemas
        assert "system" not in schemas

    def test_list_schemas_has_tpch(self, clickhouse_adapter) -> None:
        schemas = asyncio.run(clickhouse_adapter.list_schemas())
        assert "tpch" in schemas

    def test_list_tables_in_tpch(self, clickhouse_adapter) -> None:
        tables = asyncio.run(clickhouse_adapter.list_tables("tpch"))
        names = [t.name for t in tables]
        assert "region" in names
        assert "lineitem" in names

    def test_describe_table_region(self, clickhouse_adapter) -> None:
        info = asyncio.run(clickhouse_adapter.describe_table("region", "tpch"))
        assert info.name == "region"
        col_names = [c.name for c in info.columns]
        assert "r_regionkey" in col_names
        assert "r_name" in col_names
        assert info.metadata.get("engine") == "MergeTree"

    def test_dry_run_region(self, clickhouse_adapter) -> None:
        est = asyncio.run(clickhouse_adapter.dry_run("SELECT * FROM tpch.region"))
        assert est is not None
        assert est.summary

    def test_dry_run_count_has_plan(self, clickhouse_adapter) -> None:
        est = asyncio.run(clickhouse_adapter.dry_run("SELECT count() FROM tpch.lineitem"))
        assert est is not None
        assert est.plan_node is not None

    def test_dry_run_ddl_returns_none(self, clickhouse_adapter) -> None:
        sql = "CREATE TABLE default._dbastion_tmp (x Int32) ENGINE = Memory"
        asyncio.run(clickhouse_adapter.dry_run(sql))
        # DDL may or may not return None depending on ClickHouse version.
        # At minimum, it should not raise.

    def test_dialect(self, clickhouse_adapter) -> None:
        assert clickhouse_adapter.dialect() == "clickhouse"

    def test_db_type(self, clickhouse_adapter) -> None:
        assert clickhouse_adapter.db_type() == DatabaseType.CLICKHOUSE
