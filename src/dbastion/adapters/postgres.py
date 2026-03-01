"""PostgreSQL adapter — dry-run via EXPLAIN, labels via SQL comments + application_name."""

from __future__ import annotations

import json
import time

import psycopg

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


def _detect_plan_warnings(plan_node: dict) -> list[str]:
    """Walk the plan tree and flag risky operations."""
    warnings: list[str] = []
    _walk_plan(plan_node, warnings)
    return warnings


def _walk_plan(node: dict, warnings: list[str]) -> None:
    node_type = node.get("Node Type", "")
    rows = node.get("Plan Rows", 0)
    relation = node.get("Relation Name", "")

    if node_type == "Seq Scan" and rows > 100_000:
        warnings.append(f"Seq Scan on {relation} (~{_format_rows(rows)} rows)")

    for child in node.get("Plans", []):
        _walk_plan(child, warnings)


def _format_rows(n: float) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


class PostgresAdapter:
    """PostgreSQL adapter using psycopg (async)."""

    def __init__(self) -> None:
        self._conn: psycopg.AsyncConnection | None = None

    async def connect(self, config: ConnectionConfig) -> None:
        dsn = config.params.get("dsn")
        if not dsn:
            raise AdapterError("PostgreSQL requires 'dsn' in connection params")
        try:
            self._conn = await psycopg.AsyncConnection.connect(
                dsn, autocommit=True, application_name="dbastion"
            )
        except Exception as e:
            raise AdapterError(f"PostgreSQL connection failed: {e}") from e

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _ensure_conn(self) -> psycopg.AsyncConnection:
        if self._conn is None:
            raise AdapterError("Not connected. Call connect() first.")
        return self._conn

    async def dry_run(self, sql: str) -> CostEstimate | None:
        conn = self._ensure_conn()
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
                row = await cur.fetchone()
        except psycopg.Error as e:
            # SQLSTATE 42601 = syntax_error. Postgres EXPLAIN only supports
            # SELECT/INSERT/UPDATE/DELETE/MERGE — DDL produces a syntax error.
            if e.sqlstate == "42601":
                return None
            raise AdapterError(f"PostgreSQL EXPLAIN failed: {e}") from e
        except Exception as e:
            raise AdapterError(f"PostgreSQL EXPLAIN failed: {e}") from e

        if not row or not row[0]:
            return CostEstimate(summary="no plan returned")

        plan = row[0]
        if isinstance(plan, str):
            plan = json.loads(plan)

        root = plan[0]["Plan"] if plan else {}
        total_cost = root.get("Total Cost", 0.0)
        estimated_rows = root.get("Plan Rows", 0.0)
        node_type = root.get("Node Type", "")

        warnings = _detect_plan_warnings(root)

        parts = [f"cost: {total_cost:.1f} units"]
        parts.append(f"~{_format_rows(estimated_rows)} rows")
        parts.append(node_type)
        if warnings:
            parts.append(f"warnings: {', '.join(warnings)}")

        return CostEstimate(
            raw_value=total_cost,
            unit=CostUnit.COST_UNITS,
            estimated_rows=estimated_rows,
            plan_node=node_type,
            warnings=warnings,
            summary=" | ".join(parts),
        )

    async def execute(
        self, sql: str, *, labels: dict[str, str] | None = None
    ) -> ExecutionResult:
        conn = self._ensure_conn()

        # Label via SQL comment prefix.
        if labels:
            label_str = ", ".join(f"{k}={v}" for k, v in labels.items())
            sql = f"/* dbastion: {label_str} */ {sql}"

        t0 = time.monotonic()
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                columns = [desc.name for desc in cur.description] if cur.description else []
                rows_raw = await cur.fetchall() if cur.description else []
        except Exception as e:
            raise AdapterError(f"PostgreSQL execution failed: {e}") from e
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
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT table_schema, table_name "
                    "FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY table_schema, table_name"
                )
                table_rows = await cur.fetchall()

                for schema, table_name in table_rows:
                    if level == IntrospectionLevel.CATALOG:
                        tables.append(TableInfo(schema=schema, name=table_name))
                        continue

                    await cur.execute(
                        "SELECT column_name, data_type, is_nullable "
                        "FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ordinal_position",
                        (schema, table_name),
                    )
                    col_rows = await cur.fetchall()
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
            raise AdapterError(f"PostgreSQL introspection failed: {e}") from e

        return SchemaMetadata(tables=tables)

    def db_type(self) -> DatabaseType:
        return DatabaseType.POSTGRES

    def dialect(self) -> str:
        return "postgres"

    def dangerous_functions(self) -> frozenset[str]:
        return frozenset({
            # Process control
            "pg_terminate_backend",
            "pg_cancel_backend",
            # File system access
            "pg_read_file",
            "pg_read_binary_file",
            # Large object I/O
            "lo_import",
            "lo_export",
            # Advisory locks (can cause deadlocks)
            "pg_advisory_lock",
            "pg_advisory_xact_lock",
            # Config mutation
            "set_config",
            # Replication / WAL
            "pg_switch_wal",
            "pg_create_restore_point",
        })
