"""Integration tests for DuckDB adapter — runs real queries in-memory."""

import asyncio

import pytest

from dbastion.adapters._base import ConnectionConfig, CostUnit, DatabaseType
from dbastion.adapters.duckdb import DuckDBAdapter


@pytest.fixture
def adapter():
    a = DuckDBAdapter()
    config = ConnectionConfig(name="test", db_type=DatabaseType.DUCKDB, params={"path": ":memory:"})
    asyncio.run(a.connect(config))
    yield a
    asyncio.run(a.close())


def test_execute_simple(adapter):
    result = asyncio.run(adapter.execute("SELECT 1 AS x, 'hello' AS y"))
    assert result.columns == ["x", "y"]
    assert result.row_count == 1
    assert result.rows == [{"x": 1, "y": "hello"}]
    assert result.duration_ms is not None


def test_execute_with_labels(adapter):
    """Labels are prepended as SQL comments (no error)."""
    result = asyncio.run(adapter.execute("SELECT 42 AS n", labels={"tool": "test"}))
    assert result.rows == [{"n": 42}]


def test_dry_run(adapter):
    estimate = asyncio.run(adapter.dry_run("SELECT 1"))
    assert estimate.unit == CostUnit.COST_UNITS
    assert "DuckDB query plan" in estimate.summary


def test_list_schemas_empty(adapter):
    schemas = asyncio.run(adapter.list_schemas())
    # Fresh in-memory DB has no user tables, so no schemas with tables.
    assert isinstance(schemas, list)
    assert "pg_catalog" not in schemas
    assert "information_schema" not in schemas


def test_list_schemas_with_table(adapter):
    asyncio.run(adapter.execute("CREATE SCHEMA analytics"))
    asyncio.run(adapter.execute("CREATE TABLE analytics.events (id INTEGER)"))
    schemas = asyncio.run(adapter.list_schemas())
    assert "analytics" in schemas
    assert "pg_catalog" not in schemas
    assert "information_schema" not in schemas


def test_list_tables_empty(adapter):
    tables = asyncio.run(adapter.list_tables("main"))
    assert tables == []


def test_list_tables_with_table(adapter):
    asyncio.run(adapter.execute("CREATE TABLE test_tbl (id INTEGER, name VARCHAR)"))
    tables = asyncio.run(adapter.list_tables("main"))
    assert len(tables) == 1
    assert tables[0].name == "test_tbl"
    assert tables[0].schema == "main"


def test_list_tables_no_schema_arg(adapter):
    """list_tables(None) returns all tables, excluding system schemas."""
    asyncio.run(adapter.execute("CREATE SCHEMA analytics"))
    asyncio.run(adapter.execute("CREATE TABLE main.users (id INTEGER)"))
    asyncio.run(adapter.execute("CREATE TABLE analytics.events (id INTEGER)"))
    tables = asyncio.run(adapter.list_tables())
    table_names = {(t.schema, t.name) for t in tables}
    assert ("main", "users") in table_names
    assert ("analytics", "events") in table_names
    # System schemas excluded
    schemas_in_result = {t.schema for t in tables}
    assert "pg_catalog" not in schemas_in_result
    assert "information_schema" not in schemas_in_result


def test_describe_table(adapter):
    asyncio.run(adapter.execute("CREATE TABLE test_tbl (id INTEGER, name VARCHAR)"))
    info = asyncio.run(adapter.describe_table("test_tbl"))
    assert info.name == "test_tbl"
    assert info.schema == "main"
    assert len(info.columns) == 2
    assert info.columns[0].name == "id"
    assert info.columns[1].name == "name"


def test_dialect(adapter):
    assert adapter.dialect() == "duckdb"


def test_db_type(adapter):
    assert adapter.db_type() == DatabaseType.DUCKDB
