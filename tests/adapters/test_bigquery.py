"""Unit tests for BigQueryAdapter using mocked google-cloud-bigquery objects."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("google.cloud.bigquery")

from dbastion.adapters._base import CostUnit
from dbastion.adapters.bigquery import BigQueryAdapter


class _Field:
    def __init__(self, name: str) -> None:
        self.name = name


class _ResultIter:
    def __init__(self, rows: list[dict[str, object]], schema: list[_Field]) -> None:
        self._rows = rows
        self.schema = schema

    def __iter__(self):
        return iter(self._rows)


class _QueryJob:
    def __init__(
        self,
        *,
        rows: list[dict[str, object]],
        result_schema: list[_Field],
        total_bytes_processed: int,
        job_schema: list[_Field] | None,
    ) -> None:
        self._result_iter = _ResultIter(rows, result_schema)
        self.total_bytes_processed = total_bytes_processed
        # Intentionally separate from result_iter schema to catch regressions.
        self.schema = job_schema

    def result(self) -> _ResultIter:
        return self._result_iter


class _Client:
    def __init__(self, query_job: _QueryJob) -> None:
        self._query_job = query_job
        self.calls: list[tuple[str, object | None]] = []

    def query(self, sql: str, job_config=None) -> _QueryJob:
        self.calls.append((sql, job_config))
        return self._query_job

    def close(self) -> None:
        return None


def test_dangerous_functions_exists_and_empty() -> None:
    adapter = BigQueryAdapter()
    assert adapter.dangerous_functions() == frozenset()


def test_execute_uses_result_iter_schema_not_query_job_schema() -> None:
    query_job = _QueryJob(
        rows=[{"x": 1}],
        result_schema=[_Field("x")],
        total_bytes_processed=1024**3,
        job_schema=None,  # Old bug: reading this made columns empty.
    )
    client = _Client(query_job)
    adapter = BigQueryAdapter()
    adapter._client = client  # noqa: SLF001 - test-only injection

    result = asyncio.run(adapter.execute("SELECT 1"))

    assert result.columns == ["x"]
    assert result.row_count == 1
    assert result.rows == [{"x": 1}]
    assert result.cost is not None
    assert result.cost.unit == CostUnit.BYTES
    assert result.cost.estimated_gb is not None
    assert result.cost.estimated_gb > 0


def test_execute_passes_labels_to_query_job_config() -> None:
    query_job = _QueryJob(
        rows=[{"value": 7}],
        result_schema=[_Field("value")],
        total_bytes_processed=0,
        job_schema=None,
    )
    client = _Client(query_job)
    adapter = BigQueryAdapter()
    adapter._client = client  # noqa: SLF001 - test-only injection

    asyncio.run(adapter.execute("SELECT 7 AS value", labels={"tool": "test"}))

    assert len(client.calls) == 1
    _, job_config = client.calls[0]
    assert job_config is not None
    assert getattr(job_config, "labels", None) == {"tool": "test"}
