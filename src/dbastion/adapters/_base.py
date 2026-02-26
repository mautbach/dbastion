"""Database adapter protocol — the abstraction boundary between engines and drivers."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


class DatabaseType(enum.Enum):
    POSTGRES = "postgres"
    BIGQUERY = "bigquery"
    DUCKDB = "duckdb"


class CostUnit(enum.Enum):
    BYTES = "bytes"  # BigQuery
    COST_UNITS = "cost_units"  # PostgreSQL EXPLAIN
    PARTITIONS = "partitions"  # Snowflake


@dataclass
class ConnectionConfig:
    name: str
    db_type: DatabaseType
    params: dict[str, str] = field(default_factory=dict)


@dataclass
class CostEstimate:
    """Unified cost estimate across databases."""

    raw_value: float | None = None
    unit: CostUnit | None = None
    estimated_cost_usd: float | None = None
    estimated_gb: float | None = None
    estimated_rows: float | None = None
    plan_node: str | None = None  # e.g. "Seq Scan", "Index Scan"
    warnings: list[str] = field(default_factory=list)
    summary: str = ""


@dataclass
class ExecutionResult:
    """Query execution result."""

    columns: list[str]
    rows: list[dict[str, object]]
    row_count: int
    cost: CostEstimate | None = None
    duration_ms: float | None = None


class AdapterError(Exception):
    """Raised by adapters for connection/execution failures."""


class IntrospectionLevel(enum.Enum):
    CATALOG = "catalog"
    STRUCTURE = "structure"
    FULL = "full"


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    foreign_key: ForeignKeyRef | None = None
    comment: str | None = None


@dataclass
class ForeignKeyRef:
    target_table: str
    target_column: str


@dataclass
class TableInfo:
    schema: str | None
    name: str
    row_count_estimate: int | None = None
    columns: list[ColumnInfo] = field(default_factory=list)


@dataclass
class SchemaMetadata:
    tables: list[TableInfo] = field(default_factory=list)


@runtime_checkable
class DatabaseAdapter(Protocol):
    async def connect(self, config: ConnectionConfig) -> None: ...
    async def close(self) -> None: ...
    async def dry_run(self, sql: str) -> CostEstimate: ...
    async def execute(
        self, sql: str, *, labels: dict[str, str] | None = None
    ) -> ExecutionResult: ...
    async def introspect(self, level: IntrospectionLevel) -> SchemaMetadata: ...
    def db_type(self) -> DatabaseType: ...
    def dialect(self) -> str: ...
    def dangerous_functions(self) -> frozenset[str]:
        """Functions that can cause damage even inside a SELECT.

        Returns lowercase function names. Adapters override this with
        database-specific blocklists.
        """
        return frozenset()
