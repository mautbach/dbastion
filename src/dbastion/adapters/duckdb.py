"""DuckDB adapter — local/in-memory, great for testing and local analytics."""

from __future__ import annotations

import time

import duckdb as _duckdb

from dbastion.adapters._base import (
    AdapterError,
    ColumnInfo,
    ConnectionConfig,
    CostEstimate,
    CostUnit,
    DatabaseType,
    ExecutionResult,
    IntrospectionLevel,
    SchemaMetadata,
    TableInfo,
)


class DuckDBAdapter:
    """DuckDB adapter — in-process, no server needed."""

    def __init__(self) -> None:
        self._conn: _duckdb.DuckDBPyConnection | None = None

    async def connect(self, config: ConnectionConfig) -> None:
        path = config.params.get("path", ":memory:")
        try:
            self._conn = _duckdb.connect(path, config={"custom_user_agent": "dbastion/0.1.0"})
        except Exception as e:
            raise AdapterError(f"DuckDB connection failed: {e}") from e

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _ensure_conn(self) -> _duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise AdapterError("Not connected. Call connect() first.")
        return self._conn

    async def dry_run(self, sql: str) -> CostEstimate:
        conn = self._ensure_conn()
        try:
            result = conn.execute(f"EXPLAIN {sql}")
            plan = result.fetchall()
        except Exception as e:
            raise AdapterError(f"DuckDB EXPLAIN failed: {e}") from e

        plan_text = "\n".join(row[1] if len(row) > 1 else row[0] for row in plan)
        return CostEstimate(
            unit=CostUnit.COST_UNITS,
            summary=f"DuckDB query plan:\n{plan_text}",
        )

    async def execute(
        self, sql: str, *, labels: dict[str, str] | None = None
    ) -> ExecutionResult:
        conn = self._ensure_conn()

        # DuckDB has no native label support; prepend SQL comment.
        if labels:
            label_str = ", ".join(f"{k}={v}" for k, v in labels.items())
            sql = f"/* dbastion: {label_str} */ {sql}"

        t0 = time.monotonic()
        try:
            result = conn.execute(sql)
            columns = [desc[0] for desc in result.description] if result.description else []
            rows_raw = result.fetchall()
        except Exception as e:
            raise AdapterError(f"DuckDB execution failed: {e}") from e
        duration_ms = (time.monotonic() - t0) * 1000

        rows = [dict(zip(columns, row, strict=True)) for row in rows_raw]

        return ExecutionResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    async def introspect(self, level: IntrospectionLevel) -> SchemaMetadata:
        conn = self._ensure_conn()
        tables: list[TableInfo] = []

        try:
            result = conn.execute(
                "SELECT table_schema, table_name "
                "FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name"
            )
            table_rows = result.fetchall()

            for schema, table_name in table_rows:
                if level == IntrospectionLevel.CATALOG:
                    tables.append(TableInfo(schema=schema, name=table_name))
                    continue

                col_result = conn.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ? "
                    "ORDER BY ordinal_position",
                    [schema, table_name],
                )
                col_rows = col_result.fetchall()
                columns = [
                    ColumnInfo(
                        name=col_name,
                        data_type=data_type,
                        is_nullable=(nullable == "YES"),
                    )
                    for col_name, data_type, nullable in col_rows
                ]
                tables.append(
                    TableInfo(schema=schema, name=table_name, columns=columns)
                )
        except Exception as e:
            raise AdapterError(f"DuckDB introspection failed: {e}") from e

        return SchemaMetadata(tables=tables)

    def db_type(self) -> DatabaseType:
        return DatabaseType.DUCKDB

    def dialect(self) -> str:
        return "duckdb"

    def dangerous_functions(self) -> frozenset[str]:
        return frozenset()
