"""CLI integration tests for `dbastion schema` commands using in-memory DuckDB."""

from __future__ import annotations

import json
import tempfile

import duckdb
from click.testing import CliRunner

from dbastion.cli import main


def _create_test_db() -> str:
    """Create a temp DuckDB file with test tables and return the path."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
        path = f.name
    conn = duckdb.connect(path)
    conn.execute("CREATE SCHEMA analytics")
    conn.execute("CREATE TABLE main.users (id INTEGER, name VARCHAR, active BOOLEAN)")
    conn.execute("CREATE TABLE analytics.events (ts TIMESTAMP, event_type VARCHAR)")
    conn.close()
    return path


class TestSchemaLs:
    def test_list_schemas_json(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "ls", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "analytics" in data["schemas"]
        assert "main" in data["schemas"]

    def test_list_schemas_text(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "ls", "--db", f"duckdb:path={path}", "--format", "text",
        ])
        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert "analytics" in lines
        assert "main" in lines

    def test_list_tables_json(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "ls", "main", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema"] == "main"
        assert "users" in data["tables"]

    def test_list_tables_text(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "ls", "main", "--db", f"duckdb:path={path}", "--format", "text",
        ])
        assert result.exit_code == 0
        assert "users" in result.output

    def test_list_tables_analytics_schema(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "ls", "analytics", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "events" in data["tables"]

    def test_list_tables_empty_schema(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "ls", "main", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["tables"] == []


class TestSchemaShow:
    def test_show_table_json(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "show", "main.users", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema"] == "main"
        assert data["table"] == "users"
        col_names = [c["name"] for c in data["columns"]]
        assert col_names == ["id", "name", "active"]

    def test_show_table_text(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "show", "main.users", "--db", f"duckdb:path={path}", "--format", "text",
        ])
        assert result.exit_code == 0
        assert "main.users" in result.output
        assert "id" in result.output
        assert "INTEGER" in result.output

    def test_show_without_schema_defaults_to_main(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "show", "users", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema"] == "main"
        assert data["table"] == "users"

    def test_show_analytics_table(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "show", "analytics.events", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["schema"] == "analytics"
        assert data["table"] == "events"
        col_names = [c["name"] for c in data["columns"]]
        assert "ts" in col_names
        assert "event_type" in col_names

    def test_show_nonexistent_table(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "show", "main.nonexistent", "--db", "duckdb:", "--format", "json",
        ])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "error" in data

    def test_column_types_and_nullability(self) -> None:
        path = _create_test_db()
        runner = CliRunner()
        result = runner.invoke(main, [
            "schema", "show", "main.users", "--db", f"duckdb:path={path}", "--format", "json",
        ])
        data = json.loads(result.output)
        col_map = {c["name"]: c for c in data["columns"]}
        assert col_map["id"]["type"] == "INTEGER"
        assert col_map["name"]["type"] == "VARCHAR"
        assert col_map["active"]["type"] == "BOOLEAN"
