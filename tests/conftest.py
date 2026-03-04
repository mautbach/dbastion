"""Root conftest — shared fixtures and markers."""

from __future__ import annotations

import os

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "postgres: requires running PostgreSQL container")
    config.addinivalue_line("markers", "clickhouse: requires running ClickHouse container")


def pytest_collection_modifyitems(config, items):
    if not os.environ.get("DBASTION_TEST_POSTGRES"):
        skip_pg = pytest.mark.skip(reason="Postgres not available (set DBASTION_TEST_POSTGRES=1)")
        for item in items:
            if "postgres" in item.keywords:
                item.add_marker(skip_pg)

    if not os.environ.get("DBASTION_TEST_CLICKHOUSE"):
        skip_ch = pytest.mark.skip(
            reason="ClickHouse not available (set DBASTION_TEST_CLICKHOUSE=1)",
        )
        for item in items:
            if "clickhouse" in item.keywords:
                item.add_marker(skip_ch)
