"""Unit tests for BigQueryAdapter using mocked google-cloud-bigquery objects."""

from __future__ import annotations

import asyncio
import datetime
import json

import pytest

pytest.importorskip("google.cloud.bigquery")

from dbastion.adapters._base import CostUnit
from dbastion.adapters.bigquery import BigQueryAdapter


class _Field:
    def __init__(
        self,
        name: str,
        field_type: str = "STRING",
        mode: str = "NULLABLE",
        description: str | None = None,
    ) -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode
        self.description = description


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


class _Dataset:
    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id


class _TableListItem:
    def __init__(self, table_id: str) -> None:
        self.table_id = table_id


class _TimePartitioning:
    def __init__(self, type_: str, field: str | None = None) -> None:
        self.type_ = type_
        self.field = field


class _Table:
    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        schema: list[_Field],
        num_rows: int = 0,
        time_partitioning: _TimePartitioning | None = None,
        clustering_fields: list[str] | None = None,
        created: datetime.datetime | None = None,
        modified: datetime.datetime | None = None,
        num_bytes: int | None = None,
    ) -> None:
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema = schema
        self.num_rows = num_rows
        self.time_partitioning = time_partitioning
        self.clustering_fields = clustering_fields
        self.created = created
        self.modified = modified
        self.num_bytes = num_bytes


class _Client:
    def __init__(self, query_job: _QueryJob | None = None) -> None:
        self._query_job = query_job
        self.calls: list[tuple[str, object | None]] = []
        self._datasets: list[_Dataset] = []
        self._tables: dict[str, list[_TableListItem]] = {}
        self._table_details: dict[str, _Table] = {}

    def query(self, sql: str, job_config=None) -> _QueryJob:
        self.calls.append((sql, job_config))
        assert self._query_job is not None
        return self._query_job

    def list_datasets(self) -> list[_Dataset]:
        return self._datasets

    def list_tables(self, dataset: str) -> list[_TableListItem]:
        return self._tables.get(dataset, [])

    def get_table(self, ref: str) -> _Table:
        if ref not in self._table_details:
            raise Exception(f"Not found: {ref}")
        return self._table_details[ref]

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


# -- list_schemas / list_tables / describe_table --


def _make_adapter(client: _Client) -> BigQueryAdapter:
    adapter = BigQueryAdapter()
    adapter._client = client  # noqa: SLF001
    return adapter


def test_list_schemas() -> None:
    client = _Client()
    client._datasets = [_Dataset("users"), _Dataset("events")]
    adapter = _make_adapter(client)

    schemas = asyncio.run(adapter.list_schemas())
    assert schemas == ["users", "events"]


def test_list_tables() -> None:
    client = _Client()
    client._tables["users"] = [_TableListItem("profiles"), _TableListItem("sessions")]
    adapter = _make_adapter(client)

    tables = asyncio.run(adapter.list_tables("users"))
    assert len(tables) == 2
    assert tables[0].name == "profiles"
    assert tables[0].schema == "users"
    assert tables[1].name == "sessions"


def test_describe_table_with_full_metadata() -> None:
    created = datetime.datetime(2025, 5, 12, 11, 0, 0, tzinfo=datetime.UTC)
    modified = datetime.datetime(2026, 3, 1, 14, 0, 0, tzinfo=datetime.UTC)
    client = _Client()
    client._table_details["users.events"] = _Table(
        dataset_id="users",
        table_id="events",
        schema=[
            _Field("ts", "TIMESTAMP", "REQUIRED"),
            _Field("event_type", "STRING", "NULLABLE", description="Type of event"),
        ],
        num_rows=1_000_000,
        time_partitioning=_TimePartitioning("DAY", "ts"),
        clustering_fields=["event_type"],
        created=created,
        modified=modified,
        num_bytes=500_000_000,
    )
    adapter = _make_adapter(client)

    info = asyncio.run(adapter.describe_table("events", schema="users"))

    assert info.schema == "users"
    assert info.name == "events"
    assert info.row_count_estimate == 1_000_000
    assert len(info.columns) == 2
    assert info.columns[0].name == "ts"
    assert info.columns[0].data_type == "TIMESTAMP"
    assert info.columns[0].is_nullable is False  # REQUIRED
    assert info.columns[1].comment == "Type of event"

    # Metadata fields
    meta = info.metadata
    assert meta["partitioning"] == {"type": "DAY", "field": "ts"}
    assert meta["clustering"] == ["event_type"]
    assert meta["created"] == created.isoformat()
    assert meta["modified"] == modified.isoformat()
    assert meta["num_bytes"] == 500_000_000

    # Metadata must be JSON-serializable
    json.dumps(meta)


def test_describe_table_plain_no_extra_metadata() -> None:
    client = _Client()
    client._table_details["users.simple"] = _Table(
        dataset_id="users",
        table_id="simple",
        schema=[_Field("id", "INTEGER", "REQUIRED")],
        num_rows=100,
        time_partitioning=None,
        clustering_fields=None,
        created=None,
        modified=None,
        num_bytes=None,
    )
    adapter = _make_adapter(client)

    info = asyncio.run(adapter.describe_table("simple", schema="users"))

    assert info.metadata == {}
    json.dumps(info.metadata)
