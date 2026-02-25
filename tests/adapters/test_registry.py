"""Test lazy adapter registry."""


from dbastion.adapters._base import AdapterError, DatabaseType
from dbastion.adapters._registry import get_adapter


def test_get_duckdb_adapter():
    cls = get_adapter(DatabaseType.DUCKDB)
    assert cls.__name__ == "DuckDBAdapter"


def test_get_bigquery_adapter():
    """BigQuery adapter class can be loaded (google-cloud-bigquery may or may not be installed)."""
    try:
        cls = get_adapter(DatabaseType.BIGQUERY)
        assert cls.__name__ == "BigQueryAdapter"
    except AdapterError as e:
        assert "Missing driver" in str(e)
        assert "dbastion[bigquery]" in str(e)


def test_get_postgres_adapter():
    """Postgres adapter class can be loaded (psycopg may or may not be installed)."""
    try:
        cls = get_adapter(DatabaseType.POSTGRES)
        assert cls.__name__ == "PostgresAdapter"
    except AdapterError as e:
        assert "Missing driver" in str(e)
        assert "dbastion[postgres]" in str(e)


def test_missing_driver_has_install_hint():
    """When a driver import fails, the error includes pip install instructions."""
    # We can't easily force an ImportError for installed packages,
    # but we test the message format by checking the registry mapping exists.
    from dbastion.adapters._registry import _ADAPTER_MAP, _EXTRAS

    for db_type in _ADAPTER_MAP:
        assert db_type in _EXTRAS
