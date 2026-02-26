"""Adapter test fixtures."""

from __future__ import annotations

import asyncio
import os

import pytest

POSTGRES_DSN = os.environ.get(
    "DBASTION_POSTGRES_DSN",
    "postgresql://dbastion:dbastion_test@localhost:5433/dbastion_test",
)


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
