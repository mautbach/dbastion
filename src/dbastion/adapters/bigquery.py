"""BigQuery adapter — dry-run, labels, execution via google-cloud-bigquery."""

from __future__ import annotations

import time

from google.cloud import bigquery

from dbastion.adapters._base import (
    AdapterError,
    ColumnInfo,
    ConnectionConfig,
    CostEstimate,
    CostUnit,
    DatabaseType,
    ExecutionResult,
    TableInfo,
)

# BigQuery on-demand pricing: $6.25 per TB scanned.
_USD_PER_BYTE = 6.25 / (1024**4)


class BigQueryAdapter:
    """BigQuery adapter using the google-cloud-bigquery SDK."""

    def __init__(self) -> None:
        self._client: bigquery.Client | None = None
        self._location: str = "US"

    async def connect(self, config: ConnectionConfig) -> None:
        project = config.params.get("project")
        if not project:
            raise AdapterError("BigQuery requires 'project' in connection params")
        self._location = config.params.get("location", "US")

        # Load credentials: dbastion stored → ADC fallback
        from dbastion.auth import load_bigquery_credentials

        credentials = load_bigquery_credentials()
        self._client = bigquery.Client(
            project=project, location=self._location, credentials=credentials
        )

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _ensure_client(self) -> bigquery.Client:
        if self._client is None:
            raise AdapterError("Not connected. Call connect() first.")
        return self._client

    async def dry_run(self, sql: str) -> CostEstimate | None:
        client = self._ensure_client()
        job_config = bigquery.QueryJobConfig(dry_run=True, use_legacy_sql=False)
        try:
            job = client.query(sql, job_config=job_config)
        except Exception as e:
            raise AdapterError(f"BigQuery dry-run failed: {e}") from e

        total_bytes = job.total_bytes_processed or 0
        gb = total_bytes / (1024**3)
        usd = total_bytes * _USD_PER_BYTE

        return CostEstimate(
            raw_value=float(total_bytes),
            unit=CostUnit.BYTES,
            estimated_cost_usd=usd,
            estimated_gb=gb,
            summary=f"{gb:.2f} GB (~${usd:.4f})",
        )

    async def execute(
        self, sql: str, *, labels: dict[str, str] | None = None
    ) -> ExecutionResult:
        client = self._ensure_client()
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        if labels:
            job_config.labels = labels

        t0 = time.monotonic()
        try:
            query_job = client.query(sql, job_config=job_config)
            result_iter = query_job.result()
            rows = list(result_iter)
        except Exception as e:
            raise AdapterError(f"BigQuery execution failed: {e}") from e
        duration_ms = (time.monotonic() - t0) * 1000

        columns = [field.name for field in result_iter.schema] if result_iter.schema else []
        result_rows = [dict(row.items()) for row in rows]

        cost = None
        total_bytes = query_job.total_bytes_processed
        if total_bytes is not None:
            gb = total_bytes / (1024**3)
            usd = total_bytes * _USD_PER_BYTE
            cost = CostEstimate(
                raw_value=float(total_bytes),
                unit=CostUnit.BYTES,
                estimated_cost_usd=usd,
                estimated_gb=gb,
                summary=f"{gb:.2f} GB (~${usd:.4f})",
            )

        return ExecutionResult(
            columns=columns,
            rows=result_rows,
            row_count=len(result_rows),
            cost=cost,
            duration_ms=duration_ms,
        )

    async def list_schemas(self) -> list[str]:
        client = self._ensure_client()
        try:
            return [ds.dataset_id for ds in client.list_datasets()]
        except Exception as e:
            raise AdapterError(f"BigQuery list datasets failed: {e}") from e

    async def list_tables(self, schema: str | None = None) -> list[TableInfo]:
        client = self._ensure_client()
        if not schema:
            raise AdapterError("BigQuery requires a dataset name. Use `schema ls` first.")
        try:
            return [
                TableInfo(schema=schema, name=t.table_id)
                for t in client.list_tables(schema)
            ]
        except Exception as e:
            raise AdapterError(f"BigQuery list tables failed: {e}") from e

    async def describe_table(self, table: str, schema: str | None = None) -> TableInfo:
        client = self._ensure_client()
        ref = f"{schema}.{table}" if schema else table
        try:
            t = client.get_table(ref)
        except Exception as e:
            raise AdapterError(f"BigQuery describe table failed: {e}") from e

        columns = [
            ColumnInfo(
                name=field.name,
                data_type=field.field_type,
                is_nullable=(field.mode != "REQUIRED"),
                comment=field.description,
            )
            for field in t.schema
        ]

        meta: dict[str, object] = {}
        if t.time_partitioning:
            tp: dict[str, object] = {"type": t.time_partitioning.type_}
            if t.time_partitioning.field:
                tp["field"] = t.time_partitioning.field
            meta["partitioning"] = tp
        if t.clustering_fields:
            meta["clustering"] = list(t.clustering_fields)
        if t.created is not None:
            meta["created"] = t.created.isoformat()
        if t.modified is not None:
            meta["modified"] = t.modified.isoformat()
        if t.num_bytes is not None:
            meta["num_bytes"] = t.num_bytes

        return TableInfo(
            schema=schema or t.dataset_id,
            name=t.table_id,
            row_count_estimate=t.num_rows,
            columns=columns,
            metadata=meta,
        )

    def db_type(self) -> DatabaseType:
        return DatabaseType.BIGQUERY

    def dialect(self) -> str:
        return "bigquery"

    def dangerous_functions(self) -> frozenset[str]:
        return frozenset()
