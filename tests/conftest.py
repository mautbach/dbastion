"""Root conftest — shared fixtures and markers."""

from __future__ import annotations

import os

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "postgres: requires running PostgreSQL container")


def pytest_collection_modifyitems(config, items):
    if os.environ.get("DBASTION_TEST_POSTGRES"):
        return

    skip_pg = pytest.mark.skip(reason="Postgres not available (set DBASTION_TEST_POSTGRES=1)")
    for item in items:
        if "postgres" in item.keywords:
            item.add_marker(skip_pg)
