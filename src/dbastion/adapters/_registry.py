"""Lazy adapter loading — imports driver modules only when needed."""

from __future__ import annotations

import importlib

from dbastion.adapters._base import AdapterError, DatabaseAdapter, DatabaseType

_ADAPTER_MAP: dict[DatabaseType, tuple[str, str]] = {
    DatabaseType.BIGQUERY: ("dbastion.adapters.bigquery", "BigQueryAdapter"),
    DatabaseType.POSTGRES: ("dbastion.adapters.postgres", "PostgresAdapter"),
    DatabaseType.DUCKDB: ("dbastion.adapters.duckdb", "DuckDBAdapter"),
}

_EXTRAS: dict[DatabaseType, str] = {
    DatabaseType.BIGQUERY: "bigquery",
    DatabaseType.POSTGRES: "postgres",
    DatabaseType.DUCKDB: "duckdb",
}


def get_adapter(db_type: DatabaseType) -> type[DatabaseAdapter]:
    """Lazy-load an adapter class by database type.

    Raises AdapterError with install hint if the driver package is missing.
    """
    entry = _ADAPTER_MAP.get(db_type)
    if entry is None:
        raise AdapterError(f"No adapter registered for {db_type.value}")

    module_path, class_name = entry
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        extra = _EXTRAS.get(db_type, "all")
        raise AdapterError(
            f"Missing driver for {db_type.value}. "
            f"Install with: pip install 'dbastion[{extra}]'"
        ) from e

    return getattr(mod, class_name)
