"""Test CTE-aware table extraction."""

import sqlglot

from dbastion.policy.tables import extract_tables


def _parse(sql: str) -> sqlglot.exp.Expression:
    return sqlglot.parse_one(sql)


def test_simple_select():
    assert extract_tables(_parse("SELECT * FROM users")) == ["users"]


def test_join():
    assert extract_tables(_parse("SELECT * FROM users JOIN orders ON 1=1")) == ["orders", "users"]


def test_schema_qualified():
    assert extract_tables(_parse("SELECT * FROM public.users")) == ["public.users"]


def test_cte_resolved():
    sql = "WITH cte AS (SELECT * FROM customers) SELECT * FROM cte"
    assert extract_tables(_parse(sql)) == ["customers"]


def test_nested_ctes():
    sql = (
        "WITH a AS (SELECT * FROM raw_events), "
        "b AS (SELECT * FROM a JOIN users ON 1=1) "
        "SELECT * FROM b"
    )
    assert extract_tables(_parse(sql)) == ["raw_events", "users"]


def test_subquery():
    sql = "SELECT * FROM (SELECT id FROM users) AS sub"
    assert extract_tables(_parse(sql)) == ["users"]


def test_union():
    sql = "SELECT id FROM users UNION SELECT id FROM customers"
    assert extract_tables(_parse(sql)) == ["customers", "users"]


def test_insert_includes_target():
    sql = "INSERT INTO target SELECT * FROM source"
    tables = extract_tables(_parse(sql))
    assert "target" in tables
    assert "source" in tables


def test_delete_includes_target():
    sql = "DELETE FROM users WHERE id IN (SELECT id FROM blacklist)"
    tables = extract_tables(_parse(sql))
    assert "users" in tables
    assert "blacklist" in tables


def test_update_includes_target():
    sql = "UPDATE users SET name = 'x' WHERE id IN (SELECT id FROM source)"
    tables = extract_tables(_parse(sql))
    assert "users" in tables
    assert "source" in tables


def test_ddl_fallback():
    sql = "CREATE TABLE new_table AS SELECT * FROM old_table"
    tables = extract_tables(_parse(sql))
    assert "old_table" in tables


def test_pipeline_includes_tables():
    """Tables appear in the policy pipeline result."""
    from dbastion.policy import run_policy

    result = run_policy("SELECT * FROM users JOIN orders ON 1=1")
    assert result.tables == ["orders", "users"]


def test_pipeline_cte_tables():
    from dbastion.policy import run_policy

    result = run_policy("WITH t AS (SELECT * FROM raw) SELECT * FROM t")
    assert result.tables == ["raw"]
