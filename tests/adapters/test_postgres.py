"""Integration tests for PostgresAdapter against TPC-H in Docker Postgres.

Requires: docker compose up from docker/, DBASTION_TEST_POSTGRES=1.
"""

from __future__ import annotations

import asyncio

import pytest

psycopg = pytest.importorskip("psycopg")

from dbastion.adapters._base import (  # noqa: E402
    AdapterError,
    ConnectionConfig,
    CostUnit,
    DatabaseType,
)
from dbastion.adapters.postgres import PostgresAdapter  # noqa: E402

pytestmark = pytest.mark.postgres


# -- Connection lifecycle --


def test_connect_and_close(pg_dsn):
    adapter = PostgresAdapter()
    config = ConnectionConfig(name="test", db_type=DatabaseType.POSTGRES, params={"dsn": pg_dsn})
    asyncio.run(adapter.connect(config))
    asyncio.run(adapter.close())


def test_connect_bad_dsn():
    adapter = PostgresAdapter()
    config = ConnectionConfig(
        name="bad", db_type=DatabaseType.POSTGRES, params={"dsn": "postgresql://bad:bad@localhost:1/no"}
    )
    with pytest.raises(AdapterError, match="connection failed"):
        asyncio.run(adapter.connect(config))


def test_execute_before_connect():
    adapter = PostgresAdapter()
    with pytest.raises(AdapterError, match="Not connected"):
        asyncio.run(adapter.execute("SELECT 1"))


# -- execute() --


def test_execute_simple(postgres_adapter):
    result = asyncio.run(postgres_adapter.execute("SELECT 1 AS x"))
    assert result.columns == ["x"]
    assert result.row_count == 1
    assert result.rows == [{"x": 1}]
    assert result.duration_ms is not None and result.duration_ms > 0


def test_execute_tpch_region(postgres_adapter):
    result = asyncio.run(
        postgres_adapter.execute(
            "SELECT r_regionkey, r_name FROM tpch.region ORDER BY r_regionkey"
        )
    )
    assert result.row_count == 5
    names = [row["r_name"] for row in result.rows]
    assert "AFRICA" in names
    assert "EUROPE" in names
    assert "MIDDLE EAST" in names


def test_execute_tpch_join(postgres_adapter):
    sql = """
        SELECT n.n_name, r.r_name
        FROM tpch.nation n
        JOIN tpch.region r ON n.n_regionkey = r.r_regionkey
        WHERE r.r_name = 'EUROPE'
        ORDER BY n.n_name
    """
    result = asyncio.run(postgres_adapter.execute(sql))
    assert result.row_count == 5
    assert all(row["r_name"] == "EUROPE" for row in result.rows)


def test_execute_tpch_aggregation(postgres_adapter):
    sql = """
        SELECT COUNT(*) AS cnt,
               SUM(l_extendedprice) AS total_price
        FROM tpch.lineitem
        WHERE l_shipdate < DATE '1994-01-01'
    """
    result = asyncio.run(postgres_adapter.execute(sql))
    assert result.row_count == 1
    assert result.rows[0]["cnt"] > 0
    assert result.rows[0]["total_price"] > 0


# -- Labels --


def test_execute_with_labels(postgres_adapter):
    result = asyncio.run(
        postgres_adapter.execute(
            "SELECT r_name FROM tpch.region LIMIT 1",
            labels={"tool": "test", "agent": "claude"},
        )
    )
    assert result.row_count == 1


def test_application_name(postgres_adapter):
    result = asyncio.run(
        postgres_adapter.execute(
            "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
        )
    )
    assert result.rows[0]["application_name"] == "dbastion"


# -- dry_run() --


def test_dry_run_simple(postgres_adapter):
    estimate = asyncio.run(postgres_adapter.dry_run("SELECT * FROM tpch.region"))
    assert estimate.unit == CostUnit.COST_UNITS
    assert estimate.raw_value is not None and estimate.raw_value > 0
    assert estimate.plan_node != ""


def test_dry_run_index_scan(postgres_adapter):
    estimate = asyncio.run(
        postgres_adapter.dry_run("SELECT * FROM tpch.orders WHERE o_orderkey = 1")
    )
    assert estimate.plan_node in ("Index Scan", "Index Only Scan")
    assert estimate.estimated_rows is not None and estimate.estimated_rows <= 2


def test_dry_run_tpch_q1(postgres_adapter):
    """TPC-H Q1 — pricing summary report."""
    sql = """
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity) AS sum_qty,
               SUM(l_extendedprice) AS sum_base_price,
               SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
               COUNT(*) AS count_order
        FROM tpch.lineitem
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 day'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """
    estimate = asyncio.run(postgres_adapter.dry_run(sql))
    assert estimate.raw_value is not None and estimate.raw_value > 0
    assert estimate.estimated_rows is not None and estimate.estimated_rows > 0


def test_dry_run_tpch_q5(postgres_adapter):
    """TPC-H Q5 — local supplier volume (6-table join)."""
    sql = """
        SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
        FROM tpch.customer c
        JOIN tpch.orders o ON c.c_custkey = o.o_custkey
        JOIN tpch.lineitem l ON l.l_orderkey = o.o_orderkey
        JOIN tpch.supplier s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey
        JOIN tpch.nation n ON s.s_nationkey = n.n_nationkey
        JOIN tpch.region r ON n.n_regionkey = r.r_regionkey
        WHERE r.r_name = 'ASIA'
          AND o.o_orderdate >= DATE '1994-01-01'
          AND o.o_orderdate < DATE '1994-01-01' + INTERVAL '1 year'
        GROUP BY n.n_name
        ORDER BY revenue DESC
    """
    estimate = asyncio.run(postgres_adapter.dry_run(sql))
    assert estimate.raw_value is not None and estimate.raw_value > 10
    assert isinstance(estimate.warnings, list)


def test_dry_run_full_scan_warnings(postgres_adapter):
    """Full lineitem scan — warnings list is populated (may be empty at low SF)."""
    estimate = asyncio.run(postgres_adapter.dry_run("SELECT * FROM tpch.lineitem"))
    assert estimate.plan_node == "Seq Scan"
    assert isinstance(estimate.warnings, list)


# -- dry_run() errors --


def test_dry_run_invalid_sql(postgres_adapter):
    with pytest.raises(AdapterError, match="EXPLAIN failed"):
        asyncio.run(postgres_adapter.dry_run("SELECTT * FORM users"))


def test_dry_run_nonexistent_table(postgres_adapter):
    with pytest.raises(AdapterError, match="EXPLAIN failed"):
        asyncio.run(postgres_adapter.dry_run("SELECT * FROM nonexistent_table_xyz"))


# -- list_schemas / list_tables / describe_table --


def test_list_schemas(postgres_adapter):
    schemas = asyncio.run(postgres_adapter.list_schemas())
    assert "tpch" in schemas
    assert "pg_catalog" not in schemas
    assert "information_schema" not in schemas


def test_list_tables_in_schema(postgres_adapter):
    tables = asyncio.run(postgres_adapter.list_tables("tpch"))
    table_names = {t.name for t in tables}
    assert table_names >= {
        "region", "nation", "part", "supplier",
        "partsupp", "customer", "orders", "lineitem",
    }
    assert all(t.schema == "tpch" for t in tables)


def test_list_tables_all(postgres_adapter):
    tables = asyncio.run(postgres_adapter.list_tables())
    schemas = {t.schema for t in tables}
    assert "tpch" in schemas
    assert "pg_catalog" not in schemas


def test_describe_table_lineitem(postgres_adapter):
    info = asyncio.run(postgres_adapter.describe_table("lineitem", schema="tpch"))
    assert info.name == "lineitem"
    assert info.schema == "tpch"
    assert len(info.columns) == 16
    col_names = [c.name for c in info.columns]
    assert "l_orderkey" in col_names
    assert "l_shipdate" in col_names


def test_describe_table_column_types(postgres_adapter):
    info = asyncio.run(postgres_adapter.describe_table("region", schema="tpch"))
    col_map = {c.name: c for c in info.columns}
    assert "integer" in col_map["r_regionkey"].data_type.lower()
    assert "character varying" in col_map["r_name"].data_type.lower()


def test_describe_table_not_found(postgres_adapter):
    with pytest.raises(AdapterError, match="not found"):
        asyncio.run(postgres_adapter.describe_table("nonexistent_xyz", schema="tpch"))


# -- Metadata --


def test_dialect(postgres_adapter):
    assert postgres_adapter.dialect() == "postgres"


def test_db_type(postgres_adapter):
    assert postgres_adapter.db_type() == DatabaseType.POSTGRES
