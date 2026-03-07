"""Snowflake adapter — dry-run via EXPLAIN USING JSON, labels via QUERY_TAG."""

from __future__ import annotations

import json
import time

import snowflake.connector

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

_EXCLUDED_SCHEMAS = ("INFORMATION_SCHEMA",)


def _format_bytes(n: float) -> str:
    if n >= 1024**4:
        return f"{n / 1024**4:.1f} TB"
    if n >= 1024**3:
        return f"{n / 1024**3:.1f} GB"
    if n >= 1024**2:
        return f"{n / 1024**2:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{int(n)} B"


class SnowflakeAdapter:
    """Snowflake adapter using snowflake-connector-python (DB-API 2.0)."""

    def __init__(self) -> None:
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    async def connect(self, config: ConnectionConfig) -> None:
        account = config.params.get("account")
        if not account:
            raise AdapterError("Snowflake requires 'account' in connection params")
        user = config.params.get("user")
        if not user:
            raise AdapterError("Snowflake requires 'user' in connection params")

        connect_kwargs: dict[str, object] = {
            "account": account,
            "user": user,
            "application": "dbastion",
        }

        password = config.params.get("password")
        if password:
            connect_kwargs["password"] = password

        private_key_file = config.params.get("private_key_file")
        if private_key_file:
            connect_kwargs["private_key_file"] = private_key_file

        authenticator = config.params.get("authenticator")
        if authenticator:
            connect_kwargs["authenticator"] = authenticator

        for key in ("warehouse", "database", "schema", "role"):
            value = config.params.get(key)
            if value:
                connect_kwargs[key] = value

        try:
            self._conn = snowflake.connector.connect(**connect_kwargs)
        except Exception as e:
            raise AdapterError(f"Snowflake connection failed: {e}") from e

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _ensure_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None:
            raise AdapterError("Not connected. Call connect() first.")
        return self._conn

    async def dry_run(self, sql: str) -> CostEstimate | None:
        conn = self._ensure_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f"EXPLAIN USING JSON {sql}")
            row = cursor.fetchone()
        except Exception:
            return None
        finally:
            cursor.close()

        if not row or not row[0]:
            return None

        raw = row[0]
        if isinstance(raw, str):
            try:
                plan = json.loads(raw)
            except json.JSONDecodeError:
                return None
        elif isinstance(raw, dict):
            plan = raw
        else:
            return None

        global_stats = plan.get("GlobalStats", {})
        partitions_total = global_stats.get("partitionsTotal")
        partitions_assigned = global_stats.get("partitionsAssigned")
        bytes_assigned = global_stats.get("bytesAssigned")

        operations = plan.get("Operations", [])
        plan_node: str | None = None
        # Operations is [[{...}, {...}]] — list of step lists.
        if operations and isinstance(operations[0], list) and operations[0]:
            first_op = operations[0][0]
            if isinstance(first_op, dict):
                plan_node = first_op.get("operation")

        warnings: list[str] = []
        if (
            partitions_total is not None
            and partitions_assigned is not None
            and partitions_total > 0
        ):
            pct = (partitions_assigned / partitions_total) * 100
            if pct > 50 and partitions_assigned > 10:
                warnings.append(
                    f"scanning {partitions_assigned}/{partitions_total} "
                    f"partitions ({pct:.0f}%)"
                )

        summary_parts: list[str] = []
        if partitions_assigned is not None:
            summary_parts.append(
                f"{partitions_assigned}/{partitions_total or '?'} partitions"
            )
        if bytes_assigned is not None:
            summary_parts.append(_format_bytes(bytes_assigned))
        if plan_node:
            summary_parts.append(plan_node)
        if warnings:
            summary_parts.append(f"warnings: {', '.join(warnings)}")

        estimated_gb = bytes_assigned / (1024**3) if bytes_assigned is not None else None

        return CostEstimate(
            raw_value=float(partitions_assigned) if partitions_assigned is not None else None,
            unit=CostUnit.PARTITIONS,
            estimated_gb=estimated_gb,
            plan_node=plan_node,
            warnings=warnings,
            summary=" | ".join(summary_parts) if summary_parts else "no stats",
        )

    async def execute(
        self, sql: str, *, labels: dict[str, str] | None = None,
    ) -> ExecutionResult:
        conn = self._ensure_conn()
        cursor = conn.cursor()

        try:
            if labels:
                label_str = ", ".join(f"{k}={v}" for k, v in labels.items())
                tag = f"dbastion: {label_str}"
                cursor.execute(f"ALTER SESSION SET QUERY_TAG = '{tag}'")

            t0 = time.monotonic()
            cursor.execute(sql)
            columns = (
                [desc[0] for desc in cursor.description]
                if cursor.description
                else []
            )
            rows_raw = cursor.fetchall()
            duration_ms = (time.monotonic() - t0) * 1000

            if labels:
                cursor.execute("ALTER SESSION SET QUERY_TAG = ''")
        except Exception as e:
            raise AdapterError(f"Snowflake execution failed: {e}") from e
        finally:
            cursor.close()

        rows = [dict(zip(columns, row, strict=True)) for row in rows_raw]

        return ExecutionResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            duration_ms=duration_ms,
        )

    async def list_schemas(self) -> list[str]:
        conn = self._ensure_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' "
                "ORDER BY SCHEMA_NAME"
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            raise AdapterError(f"Snowflake list schemas failed: {e}") from e
        finally:
            cursor.close()

    async def list_tables(self, schema: str | None = None) -> list[TableInfo]:
        conn = self._ensure_conn()
        cursor = conn.cursor()
        try:
            if schema:
                cursor.execute(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = %s "
                    "ORDER BY TABLE_NAME",
                    (schema,),
                )
                return [
                    TableInfo(schema=schema, name=row[0])
                    for row in cursor.fetchall()
                ]
            else:
                cursor.execute(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' "
                    "ORDER BY TABLE_SCHEMA, TABLE_NAME"
                )
                return [
                    TableInfo(schema=row[0], name=row[1])
                    for row in cursor.fetchall()
                ]
        except Exception as e:
            raise AdapterError(f"Snowflake list tables failed: {e}") from e
        finally:
            cursor.close()

    async def describe_table(self, table: str, schema: str | None = None) -> TableInfo:
        conn = self._ensure_conn()
        effective_schema = schema or "PUBLIC"

        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION",
                (effective_schema, table),
            )
            col_rows = cursor.fetchall()
        except Exception as e:
            raise AdapterError(f"Snowflake describe table failed: {e}") from e
        finally:
            cursor.close()

        if not col_rows:
            raise AdapterError(f"Table '{effective_schema}.{table}' not found")

        columns = [
            ColumnInfo(
                name=row[0],
                data_type=row[1],
                is_nullable=(row[2] == "YES"),
                comment=row[3] if row[3] else None,
            )
            for row in col_rows
        ]

        metadata: dict[str, JsonValue] = {}
        row_count: int | None = None
        cursor2 = conn.cursor()
        try:
            cursor2.execute(
                "SELECT ROW_COUNT, BYTES, CREATED, LAST_ALTERED, "
                "CLUSTERING_KEY, COMMENT "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (effective_schema, table),
            )
            tbl_row = cursor2.fetchone()
            if tbl_row:
                row_count = int(tbl_row[0]) if tbl_row[0] is not None else None
                if tbl_row[1] is not None:
                    metadata["bytes"] = int(tbl_row[1])
                if tbl_row[2] is not None:
                    metadata["created"] = str(tbl_row[2])
                if tbl_row[3] is not None:
                    metadata["last_altered"] = str(tbl_row[3])
                if tbl_row[4]:
                    metadata["clustering_key"] = tbl_row[4]
                if tbl_row[5]:
                    metadata["comment"] = tbl_row[5]
        except Exception:
            pass
        finally:
            cursor2.close()

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
        return DatabaseType.SNOWFLAKE

    def dialect(self) -> str:
        return "snowflake"

    def dangerous_functions(self) -> frozenset[str]:
        return frozenset()
