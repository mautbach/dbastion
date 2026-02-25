"""Integration tests for DuckDB adapter — runs real queries in-memory."""

import asyncio

import pytest

from dbastion.adapters._base import ConnectionConfig, CostUnit, DatabaseType, IntrospectionLevel
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


def test_introspect_empty(adapter):
    meta = asyncio.run(adapter.introspect(IntrospectionLevel.CATALOG))
    # Fresh in-memory DB has no user tables.
    assert meta.tables == []


def test_introspect_with_table(adapter):
    asyncio.run(adapter.execute("CREATE TABLE test_tbl (id INTEGER, name VARCHAR)"))
    meta = asyncio.run(adapter.introspect(IntrospectionLevel.STRUCTURE))
    assert len(meta.tables) == 1
    tbl = meta.tables[0]
    assert tbl.name == "test_tbl"
    assert len(tbl.columns) == 2
    assert tbl.columns[0].name == "id"


def test_dialect(adapter):
    assert adapter.dialect() == "duckdb"


def test_db_type(adapter):
    assert adapter.db_type() == DatabaseType.DUCKDB
