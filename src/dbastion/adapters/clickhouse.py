"""ClickHouse adapter — dry-run via EXPLAIN ESTIMATE, labels via log_comment."""

from __future__ import annotations

import json
import time
from contextlib import suppress

import clickhouse_connect

from dbastion.adapters._base import (
    AdapterError,
    ColumnInfo,
    ConnectionConfig,
    CostEstimate,
    CostUnit,
    DatabaseType,
    ExecutionResult,
    JsonValue,
    TableInfo,
)

_EXCLUDED_SCHEMAS = ("system", "information_schema", "INFORMATION_SCHEMA")


def _format_rows(n: float) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def _parse_explain_json(text: str) -> dict | list | None:
    """Parse EXPLAIN json=1 payload from ClickHouse.

    `client.query()` usually returns raw JSON text, while `client.command()` may
    return escaped JSON (e.g. "\\n") depending on output format. Handle both.
    """
    payload = text.strip()
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        try:
            unescaped = payload.encode("utf-8").decode("unicode_escape")
            parsed = json.loads(unescaped)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
    return parsed if isinstance(parsed, (dict, list)) else None


def _extract_plan_node(parsed: dict | list) -> str | None:
    """Extract top-level node name from parsed EXPLAIN JSON payload."""
    if isinstance(parsed, dict) and "Plan" in parsed:
        plan = parsed.get("Plan")
        if isinstance(plan, dict):
            node = plan.get("Node Type")
            if isinstance(node, str):
                return node
    if isinstance(parsed, list) and parsed:
        first = parsed[0]
        if isinstance(first, dict):
            plan = first.get("Plan")
            if isinstance(plan, dict):
                node = plan.get("Node Type")
                if isinstance(node, str):
                    return node
    return None


class ClickHouseAdapter:
    """ClickHouse adapter using clickhouse-connect (HTTP)."""

    def __init__(self) -> None:
        self._client: clickhouse_connect.driver.Client | None = None

    async def connect(self, config: ConnectionConfig) -> None:
        host = config.params.get("host")
        if not host:
            raise AdapterError("ClickHouse requires 'host' in connection params")
        port = int(config.params.get("port", "8123"))
        username = config.params.get("username", "default")
        password = config.params.get("password", "")
        database = config.params.get("database", "default")
        secure = config.params.get("secure", "false").lower() in ("true", "1", "yes")

        try:
            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure,
                client_name="dbastion",
            )
        except Exception as e:
            raise AdapterError(f"ClickHouse connection failed: {e}") from e

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _ensure_client(self) -> clickhouse_connect.driver.Client:
        if self._client is None:
            raise AdapterError("Not connected. Call connect() first.")
        return self._client

    async def dry_run(self, sql: str) -> CostEstimate | None:
        client = self._ensure_client()

        estimated_rows: float | None = None
        parts: int | None = None
        plan_node: str | None = None
        warnings: list[str] = []

        # Step 1: EXPLAIN ESTIMATE — row/part counts (MergeTree only).
        try:
            result = client.query(f"EXPLAIN ESTIMATE {sql}")
            total_rows = 0.0
            total_parts = 0
            for row in result.result_rows:
                # Columns: database, table, parts, rows, marks
                total_parts += int(row[2])
                total_rows += float(row[3])
            if total_rows > 0:
                estimated_rows = total_rows
                parts = total_parts
        except Exception:
            # Non-MergeTree tables, DDL, etc. — fall through.
            pass

        # Step 2: EXPLAIN PLAN json=1 — plan tree structure.
        plan_text: str | None = None
        try:
            plan_result = client.query(f"EXPLAIN json=1 {sql}")
            if plan_result.result_rows:
                raw = plan_result.result_rows[0][0]
                if isinstance(raw, str):
                    plan_text = raw
        except Exception:
            # DDL or unsupported — fall through.
            pass

        if not plan_text:
            # Some server/client combos return escaped payload via command().
            with suppress(Exception):
                plan_text = client.command(f"EXPLAIN json=1 {sql}")

        if plan_text:
            parsed = _parse_explain_json(plan_text)
            if parsed is not None:
                plan_node = _extract_plan_node(parsed)

        # If neither EXPLAIN worked, we can't estimate.
        if estimated_rows is None and plan_node is None:
            return None

        # Warn on large scans.
        if estimated_rows is not None and estimated_rows > 1_000_000:
            warnings.append(f"large scan: ~{_format_rows(estimated_rows)} rows")

        # Build summary.
        summary_parts = []
        if estimated_rows is not None:
            summary_parts.append(f"~{_format_rows(estimated_rows)} rows")
        if parts is not None:
            summary_parts.append(f"{parts} parts")
        if plan_node:
            summary_parts.append(plan_node)
        if warnings:
            summary_parts.append(f"warnings: {', '.join(warnings)}")

        return CostEstimate(
            raw_value=estimated_rows,
            unit=CostUnit.COST_UNITS,
            estimated_rows=estimated_rows,
            plan_node=plan_node,
            warnings=warnings,
            summary=" | ".join(summary_parts),
        )

    # Columns returned by ClickHouse for mutations (DELETE/ALTER) via query().
    # These are internal progress metadata, not user-facing results.
    _PROGRESS_COLUMNS = frozenset({
        "read_rows", "read_bytes", "written_rows", "written_bytes",
        "total_rows_to_read", "result_rows", "result_bytes",
        "elapsed_ns", "query_id",
    })

    async def execute(
        self, sql: str, *, labels: dict[str, str] | None = None,
    ) -> ExecutionResult:
        client = self._ensure_client()

        settings: dict[str, str] = {}
        if labels:
            label_str = ", ".join(f"{k}={v}" for k, v in labels.items())
            settings["log_comment"] = f"dbastion: {label_str}"

        t0 = time.monotonic()
        try:
            result = client.query(sql, settings=settings)
            columns = list(result.column_names)
            rows_raw = result.result_rows
        except Exception as e:
            raise AdapterError(f"ClickHouse execution failed: {e}") from e
        duration_ms = (time.monotonic() - t0) * 1000

        # ClickHouse returns internal progress metadata for mutations
        # (DELETE, ALTER, etc.) — not meaningful for the user.
        if set(columns) <= self._PROGRESS_COLUMNS:
            return ExecutionResult(
                columns=[], rows=[], row_count=0, duration_ms=duration_ms,
            )

        rows = [dict(zip(columns, row, strict=True)) for row in rows_raw]

        return ExecutionResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    async def list_schemas(self) -> list[str]:
        client = self._ensure_client()
        try:
            result = client.query(
                "SELECT name FROM system.databases "
                "WHERE name NOT IN ({excluded:Array(String)}) "
                "ORDER BY name",
                parameters={"excluded": list(_EXCLUDED_SCHEMAS)},
            )
            return [row[0] for row in result.result_rows]
        except Exception as e:
            raise AdapterError(f"ClickHouse list schemas failed: {e}") from e

    async def list_tables(self, schema: str | None = None) -> list[TableInfo]:
        client = self._ensure_client()
        try:
            if schema:
                result = client.query(
                    "SELECT name FROM system.tables "
                    "WHERE database = {db:String} ORDER BY name",
                    parameters={"db": schema},
                )
                return [TableInfo(schema=schema, name=row[0]) for row in result.result_rows]
            else:
                result = client.query(
                    "SELECT database, name FROM system.tables "
                    "WHERE database NOT IN ({excluded:Array(String)}) "
                    "ORDER BY database, name",
                    parameters={"excluded": list(_EXCLUDED_SCHEMAS)},
                )
                return [
                    TableInfo(schema=row[0], name=row[1])
                    for row in result.result_rows
                ]
        except Exception as e:
            raise AdapterError(f"ClickHouse list tables failed: {e}") from e

    async def describe_table(self, table: str, schema: str | None = None) -> TableInfo:
        client = self._ensure_client()
        effective_schema = schema or "default"

        # Column metadata.
        try:
            col_result = client.query(
                "SELECT name, type, is_in_primary_key, comment "
                "FROM system.columns "
                "WHERE database = {db:String} AND table = {tbl:String} "
                "ORDER BY position",
                parameters={"db": effective_schema, "tbl": table},
            )
        except Exception as e:
            raise AdapterError(f"ClickHouse describe table failed: {e}") from e

        if not col_result.result_rows:
            raise AdapterError(f"Table '{effective_schema}.{table}' not found")

        columns = [
            ColumnInfo(
                name=row[0],
                data_type=row[1],
                is_nullable=row[1].startswith("Nullable("),
                is_primary_key=bool(row[2]),
                comment=row[3] if row[3] else None,
            )
            for row in col_result.result_rows
        ]

        # Table-level metadata.
        row_count: int | None = None
        metadata: dict[str, JsonValue] = {}
        try:
            tbl_result = client.query(
                "SELECT engine, total_rows, total_bytes, "
                "partition_key, sorting_key, primary_key "
                "FROM system.tables "
                "WHERE database = {db:String} AND name = {tbl:String}",
                parameters={"db": effective_schema, "tbl": table},
            )
            if tbl_result.result_rows:
                tbl = tbl_result.result_rows[0]
                metadata["engine"] = tbl[0]
                row_count = int(tbl[1]) if tbl[1] is not None else None
                if tbl[2] is not None:
                    metadata["total_bytes"] = int(tbl[2])
                if tbl[3]:
                    metadata["partition_key"] = tbl[3]
                if tbl[4]:
                    metadata["sorting_key"] = tbl[4]
                if tbl[5]:
                    metadata["primary_key"] = tbl[5]
        except Exception:
            pass  # Best-effort metadata enrichment.

        return TableInfo(
            schema=effective_schema,
            name=table,
            row_count_estimate=row_count,
            columns=columns,
            metadata=metadata,
        )

    def supports_dry_run_for(self, classification: str) -> bool:
        return True

    def db_type(self) -> DatabaseType:
        return DatabaseType.CLICKHOUSE

    def dialect(self) -> str:
        return "clickhouse"

    def dangerous_functions(self) -> frozenset[str]:
        return frozenset({
            # Remote server access
            "remote",
            "remotesecure",
            # HTTP URL access
            "url",
            "urlcluster",
            # External database access
            "mysql",
            "postgresql",
            "mongodb",
            "redis",
            "sqlite",
            "odbc",
            "jdbc",
            # Filesystem access
            "file",
            "input",
            # External program execution
            "executable",
            "executablepool",
            # Cloud storage access
            "s3",
            "s3cluster",
            "gcs",
            "azureblobstorage",
            "azureblobstoragecluster",
            # HDFS
            "hdfs",
            "hdfscluster",
        })
