"""Adapter test fixtures."""

from __future__ import annotations

import asyncio
import os

import pytest

POSTGRES_DSN = os.environ.get(
    "DBASTION_POSTGRES_DSN",
    "postgresql://dbastion:dbastion_test@localhost:5433/dbastion_test",
)

CLICKHOUSE_HOST = os.environ.get("DBASTION_CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.environ.get("DBASTION_CLICKHOUSE_PORT", "8123")

SNOWFLAKE_ACCOUNT = os.environ.get("DBASTION_SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.environ.get("DBASTION_SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.environ.get("DBASTION_SNOWFLAKE_PASSWORD", "")


@pytest.fixture(scope="session")
def pg_dsn():
    return POSTGRES_DSN


@pytest.fixture
def postgres_adapter(pg_dsn):
    """Connected PostgresAdapter, tears down after each test."""
    from dbastion.adapters._base import ConnectionConfig, DatabaseType
    from dbastion.adapters.postgres import PostgresAdapter

    adapter = PostgresAdapter()
    config = ConnectionConfig(
        name="tpch-test",
        db_type=DatabaseType.POSTGRES,
        params={"dsn": pg_dsn},
    )
    asyncio.run(adapter.connect(config))
    yield adapter
    asyncio.run(adapter.close())


@pytest.fixture(scope="session")
def sf_account():
    return SNOWFLAKE_ACCOUNT


@pytest.fixture(scope="session")
def sf_user():
    return SNOWFLAKE_USER


@pytest.fixture
def snowflake_adapter(sf_account, sf_user):
    """Connected SnowflakeAdapter, tears down after each test."""
    from dbastion.adapters._base import ConnectionConfig, DatabaseType
    from dbastion.adapters.snowflake import SnowflakeAdapter

    adapter = SnowflakeAdapter()
    config = ConnectionConfig(
        name="sf-test",
        db_type=DatabaseType.SNOWFLAKE,
        params={
            "account": sf_account,
            "user": sf_user,
            "password": SNOWFLAKE_PASSWORD,
            "warehouse": os.environ.get("DBASTION_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.environ.get("DBASTION_SNOWFLAKE_DATABASE", ""),
        },
    )
    asyncio.run(adapter.connect(config))
    yield adapter
    asyncio.run(adapter.close())


@pytest.fixture(scope="session")
def ch_host():
    return CLICKHOUSE_HOST


@pytest.fixture(scope="session")
def ch_port():
    return CLICKHOUSE_PORT


@pytest.fixture
def clickhouse_adapter(ch_host, ch_port):
    """Connected ClickHouseAdapter, tears down after each test."""
    from dbastion.adapters._base import ConnectionConfig, DatabaseType
    from dbastion.adapters.clickhouse import ClickHouseAdapter

    adapter = ClickHouseAdapter()
    config = ConnectionConfig(
        name="ch-test",
        db_type=DatabaseType.CLICKHOUSE,
        params={
            "host": ch_host,
            "port": ch_port,
            "username": "dbastion",
            "password": "dbastion_test",
            "database": "default",
        },
    )
    asyncio.run(adapter.connect(config))
    yield adapter
    asyncio.run(adapter.close())
