"""Unit tests for SnowflakeAdapter using mocked snowflake-connector-python objects."""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("snowflake.connector")

from dbastion.adapters._base import CostUnit, DatabaseType
from dbastion.adapters.snowflake import SnowflakeAdapter

# -- Mock objects --


class _Cursor:
    """Minimal mock of snowflake.connector.cursor.SnowflakeCursor."""

    def __init__(self, conn: _Connection) -> None:
        self._conn = conn
        self._results: list[tuple] = []
        self._description: list[tuple[str, ...]] | None = None
        self.executed: list[str] = []

    @property
    def description(self) -> list[tuple[str, ...]] | None:
        return self._description

    def execute(self, sql: str, params=None) -> None:
        self.executed.append(sql)
        self._conn._dispatch(self, sql)

    def fetchone(self) -> tuple | None:
        return self._results[0] if self._results else None

    def fetchall(self) -> list[tuple]:
        return self._results

    def close(self) -> None:
        pass


class _Connection:
    """Minimal mock of snowflake.connector.SnowflakeConnection."""

    def __init__(self) -> None:
        self._query_results: dict[str, tuple[list[tuple[str, ...]] | None, list[tuple]]] = {}
        self._cursors: list[_Cursor] = []

    def set_result(
        self,
        prefix: str,
        description: list[tuple[str, ...]] | None,
        rows: list[tuple],
    ) -> None:
        self._query_results[prefix] = (description, rows)

    def cursor(self) -> _Cursor:
        c = _Cursor(self)
        self._cursors.append(c)
        return c

    def _dispatch(self, cursor: _Cursor, sql: str) -> None:
        for prefix, (desc, rows) in self._query_results.items():
            if prefix in sql:
                cursor._description = desc
                cursor._results = rows
                return
        cursor._description = None
        cursor._results = []

    def close(self) -> None:
        pass


def _make_adapter(conn: _Connection) -> SnowflakeAdapter:
    adapter = SnowflakeAdapter()
    adapter._conn = conn
    return adapter


# -- Metadata tests --


class TestMetadata:
    def test_db_type(self) -> None:
        adapter = SnowflakeAdapter()
        assert adapter.db_type() == DatabaseType.SNOWFLAKE

    def test_dialect(self) -> None:
        adapter = SnowflakeAdapter()
        assert adapter.dialect() == "snowflake"

    def test_dangerous_functions_empty(self) -> None:
        adapter = SnowflakeAdapter()
        assert adapter.dangerous_functions() == frozenset()


# -- Connect tests --


class TestConnect:
    def test_connect_missing_account(self) -> None:
        from dbastion.adapters._base import AdapterError, ConnectionConfig

        adapter = SnowflakeAdapter()
        config = ConnectionConfig(
            name="bad",
            db_type=DatabaseType.SNOWFLAKE,
            params={"user": "test_user"},
        )
        with pytest.raises(AdapterError, match="account"):
            asyncio.run(adapter.connect(config))

    def test_connect_missing_user(self) -> None:
        from dbastion.adapters._base import AdapterError, ConnectionConfig

        adapter = SnowflakeAdapter()
        config = ConnectionConfig(
            name="bad",
            db_type=DatabaseType.SNOWFLAKE,
            params={"account": "test_account"},
        )
        with pytest.raises(AdapterError, match="user"):
            asyncio.run(adapter.connect(config))

    def test_not_connected_raises(self) -> None:
        from dbastion.adapters._base import AdapterError

        adapter = SnowflakeAdapter()
        with pytest.raises(AdapterError, match="Not connected"):
            asyncio.run(adapter.execute("SELECT 1"))


# -- Execute tests --


class TestExecute:
    def test_execute_simple(self) -> None:
        conn = _Connection()
        conn.set_result("SELECT", [("x",), ("y",)], [(1, "hello"), (2, "world")])
        adapter = _make_adapter(conn)

        result = asyncio.run(adapter.execute("SELECT 1 AS x, 'hello' AS y"))
        assert result.columns == ["x", "y"]
        assert result.row_count == 2
        assert result.rows[0] == {"x": 1, "y": "hello"}
        assert result.duration_ms is not None

    def test_execute_with_labels_sets_query_tag(self) -> None:
        conn = _Connection()
        conn.set_result("SELECT", [("x",)], [(1,)])
        adapter = _make_adapter(conn)

        asyncio.run(adapter.execute(
            "SELECT 1 AS x",
            labels={"tool": "dbastion", "agent": "test"},
        ))

        all_executed = []
        for c in conn._cursors:
            all_executed.extend(c.executed)
        tag_sets = [s for s in all_executed if "QUERY_TAG" in s]
        assert len(tag_sets) == 2  # SET and RESET
        assert "dbastion" in tag_sets[0]
        assert "agent=test" in tag_sets[0]
        assert tag_sets[1] == "ALTER SESSION SET QUERY_TAG = ''"

    def test_execute_without_labels_no_query_tag(self) -> None:
        conn = _Connection()
        conn.set_result("SELECT", [("x",)], [(42,)])
        adapter = _make_adapter(conn)

        asyncio.run(adapter.execute("SELECT 42 AS x"))

        all_executed = []
        for c in conn._cursors:
            all_executed.extend(c.executed)
        tag_calls = [s for s in all_executed if "QUERY_TAG" in s]
        assert tag_calls == []


# -- Dry-run tests --


class TestDryRun:
    def test_dry_run_with_partition_stats(self) -> None:
        explain_json = json.dumps({
            "GlobalStats": {
                "partitionsTotal": 100,
                "partitionsAssigned": 5,
                "bytesAssigned": 1048576,
            },
            "Operations": [
                [{"id": 0, "operation": "TableScan", "partitionsAssigned": 5}],
            ],
        })
        conn = _Connection()
        conn.set_result("EXPLAIN", [("jsonplan",)], [(explain_json,)])
        adapter = _make_adapter(conn)

        est = asyncio.run(adapter.dry_run("SELECT * FROM t"))
        assert est is not None
        assert est.unit == CostUnit.PARTITIONS
        assert est.raw_value == 5.0
        assert est.plan_node == "TableScan"
        assert "5/100 partitions" in est.summary
        assert "1.0 MB" in est.summary
        assert est.warnings == []

    def test_dry_run_large_scan_warns(self) -> None:
        explain_json = json.dumps({
            "GlobalStats": {
                "partitionsTotal": 100,
                "partitionsAssigned": 80,
                "bytesAssigned": 10737418240,
            },
            "Operations": [
                [{"id": 0, "operation": "TableScan"}],
            ],
        })
        conn = _Connection()
        conn.set_result("EXPLAIN", [("jsonplan",)], [(explain_json,)])
        adapter = _make_adapter(conn)

        est = asyncio.run(adapter.dry_run("SELECT * FROM big_table"))
        assert est is not None
        assert len(est.warnings) > 0
        assert "80/100" in est.warnings[0]

    def test_dry_run_ddl_returns_none(self) -> None:
        conn = _Connection()
        adapter = _make_adapter(conn)
        est = asyncio.run(adapter.dry_run("CREATE TABLE foo (x INT)"))
        assert est is None

    def test_dry_run_json_already_parsed(self) -> None:
        plan = {
            "GlobalStats": {
                "partitionsTotal": 50,
                "partitionsAssigned": 2,
                "bytesAssigned": 4096,
            },
            "Operations": [
                [{"id": 0, "operation": "IndexScan"}],
            ],
        }
        conn = _Connection()
        conn.set_result("EXPLAIN", [("jsonplan",)], [(plan,)])
        adapter = _make_adapter(conn)

        est = asyncio.run(adapter.dry_run("SELECT * FROM t WHERE id = 1"))
        assert est is not None
        assert est.plan_node == "IndexScan"
        assert est.raw_value == 2.0

    def test_dry_run_no_operations(self) -> None:
        explain_json = json.dumps({
            "GlobalStats": {
                "partitionsTotal": 10,
                "partitionsAssigned": 3,
                "bytesAssigned": 2048,
            },
        })
        conn = _Connection()
        conn.set_result("EXPLAIN", [("jsonplan",)], [(explain_json,)])
        adapter = _make_adapter(conn)

        est = asyncio.run(adapter.dry_run("SELECT * FROM t"))
        assert est is not None
        assert est.plan_node is None
        assert est.raw_value == 3.0


# -- Introspection tests --


class TestIntrospection:
    def test_list_schemas(self) -> None:
        conn = _Connection()
        conn.set_result(
            "INFORMATION_SCHEMA.SCHEMATA",
            [("SCHEMA_NAME",)],
            [("PUBLIC",), ("ANALYTICS",), ("RAW",)],
        )
        adapter = _make_adapter(conn)

        schemas = asyncio.run(adapter.list_schemas())
        assert schemas == ["PUBLIC", "ANALYTICS", "RAW"]

    def test_list_tables_with_schema(self) -> None:
        conn = _Connection()
        conn.set_result(
            "INFORMATION_SCHEMA.TABLES",
            [("TABLE_NAME",)],
            [("USERS",), ("EVENTS",)],
        )
        adapter = _make_adapter(conn)

        tables = asyncio.run(adapter.list_tables("PUBLIC"))
        assert len(tables) == 2
        assert tables[0].schema == "PUBLIC"
        assert tables[0].name == "USERS"
        assert tables[1].name == "EVENTS"

    def test_list_tables_all(self) -> None:
        conn = _Connection()
        conn.set_result(
            "INFORMATION_SCHEMA.TABLES",
            [("TABLE_SCHEMA",), ("TABLE_NAME",)],
            [("PUBLIC", "USERS"), ("ANALYTICS", "EVENTS")],
        )
        adapter = _make_adapter(conn)

        tables = asyncio.run(adapter.list_tables())
        assert len(tables) == 2
        assert tables[0].schema == "PUBLIC"
        assert tables[1].schema == "ANALYTICS"

    def test_describe_table(self) -> None:
        conn = _Connection()
        conn.set_result(
            "INFORMATION_SCHEMA.COLUMNS",
            [("COLUMN_NAME",), ("DATA_TYPE",), ("IS_NULLABLE",), ("COMMENT",)],
            [
                ("ID", "NUMBER", "NO", None),
                ("NAME", "VARCHAR", "YES", None),
                ("EMAIL", "VARCHAR", "YES", "user email address"),
            ],
        )
        conn.set_result(
            "ROW_COUNT",
            [("ROW_COUNT",), ("BYTES",), ("CREATED",), ("LAST_ALTERED",),
             ("CLUSTERING_KEY",), ("COMMENT",)],
            [(1000, 65536, "2025-01-01 00:00:00", "2026-03-01 12:00:00",
              "LINEAR(ID)", "Main users table")],
        )
        adapter = _make_adapter(conn)

        info = asyncio.run(adapter.describe_table("USERS", "PUBLIC"))
        assert info.schema == "PUBLIC"
        assert info.name == "USERS"
        assert info.row_count_estimate == 1000
        assert len(info.columns) == 3
        assert info.columns[0].name == "ID"
        assert info.columns[0].data_type == "NUMBER"
        assert info.columns[0].is_nullable is False
        assert info.columns[1].is_nullable is True
        assert info.columns[2].comment == "user email address"
        assert info.metadata["clustering_key"] == "LINEAR(ID)"
        assert info.metadata["comment"] == "Main users table"
        assert info.metadata["bytes"] == 65536

    def test_describe_table_not_found(self) -> None:
        conn = _Connection()
        adapter = _make_adapter(conn)

        with pytest.raises(Exception, match="not found"):
            asyncio.run(adapter.describe_table("NONEXISTENT", "PUBLIC"))

    def test_describe_table_default_schema_is_public(self) -> None:
        conn = _Connection()
        conn.set_result(
            "INFORMATION_SCHEMA.COLUMNS",
            [("COLUMN_NAME",), ("DATA_TYPE",), ("IS_NULLABLE",), ("COMMENT",)],
            [("ID", "NUMBER", "NO", None)],
        )
        adapter = _make_adapter(conn)

        info = asyncio.run(adapter.describe_table("USERS"))
        assert info.schema == "PUBLIC"
