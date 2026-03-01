"""Database adapters — implementations of the DatabaseAdapter protocol."""

from dbastion.adapters._base import (
    AdapterError,
    ColumnInfo,
    ConnectionConfig,
    CostEstimate,
    CostUnit,
    DatabaseAdapter,
    DatabaseType,
    ExecutionResult,
    ForeignKeyRef,
    TableInfo,
)

__all__ = [
    "AdapterError",
    "ColumnInfo",
    "ConnectionConfig",
    "CostEstimate",
    "CostUnit",
    "DatabaseAdapter",
    "DatabaseType",
    "ExecutionResult",
    "ForeignKeyRef",
    "TableInfo",
]
