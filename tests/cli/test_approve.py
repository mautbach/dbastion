"""CLI tests for the approve command."""

from __future__ import annotations

import json
import tempfile

import duckdb
from click.testing import CliRunner

from dbastion.cli import main


def _parse_approve_json(output: str) -> dict:
    """Extract the JSON result from approve output (skips summary text)."""
    # approve prints a summary to stderr, then JSON to stdout.
    # CliRunner merges both — find the JSON object.
    start = output.index("{")
    return json.loads(output[start:])


def _make_ask_envelope(
    *,
    effective_sql: str = "SELECT 1",
    db: str = "duckdb:",
    classification: str = "read",
    original_sql: str | None = None,
    tables: list[str] | None = None,
    cost_error: str | None = None,
) -> str:
    """Build a minimal ask-envelope JSON string."""
    envelope: dict[str, object] = {
        "decision": "ask",
        "effective_sql": effective_sql,
        "db": db,
        "classification": classification,
    }
    if original_sql:
        envelope["original_sql"] = original_sql
    if tables:
        envelope["tables"] = tables
    if cost_error:
        envelope["cost_error"] = cost_error
    return json.dumps(envelope)


class TestApproveDecisionGuards:
    """approve rejects deny/allow envelopes, only accepts ask."""

    def test_deny_envelope_rejected(self) -> None:
        runner = CliRunner()
        envelope = json.dumps({"decision": "deny", "effective_sql": "DROP TABLE x"})
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "denied by policy" in data["error"]

    def test_allow_envelope_no_op(self) -> None:
        runner = CliRunner()
        envelope = json.dumps({"decision": "allow", "effective_sql": "SELECT 1"})
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "already allowed" in data["error"]

    def test_unknown_decision_rejected(self) -> None:
        runner = CliRunner()
        envelope = json.dumps({"decision": "maybe", "effective_sql": "SELECT 1"})
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "Unexpected decision" in data["error"]


class TestApproveMissingFields:
    """approve rejects envelopes missing required fields."""

    def test_missing_db(self) -> None:
        runner = CliRunner()
        envelope = json.dumps({"decision": "ask", "effective_sql": "SELECT 1"})
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "missing 'db'" in data["error"].lower() or "db" in data["error"].lower()

    def test_missing_effective_sql(self) -> None:
        runner = CliRunner()
        envelope = json.dumps({"decision": "ask", "db": "duckdb:"})
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert "effective_sql" in data["error"]

    def test_empty_stdin(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["approve"], input="")
        assert result.exit_code != 0
        assert "empty" in result.output.lower()

    def test_invalid_json(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["approve"], input="not json{")
        assert result.exit_code != 0
        assert "Invalid JSON" in result.output


class TestApproveExecution:
    """approve executes ask-envelope queries (no-TTY mode)."""

    def test_read_executes(self) -> None:
        runner = CliRunner()
        envelope = _make_ask_envelope(effective_sql="SELECT 42 AS answer")
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 0
        data = _parse_approve_json(result.output)
        assert data["decision"] == "approved"
        assert data["columns"] == ["answer"]
        assert data["rows"] == [{"answer": 42}]

    def test_write_executes(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
            db_path = f.name
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.close()

        envelope = _make_ask_envelope(
            effective_sql="INSERT INTO t VALUES (1)",
            db=f"duckdb:path={db_path}",
            classification="dml",
        )
        runner = CliRunner()
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 0
        data = _parse_approve_json(result.output)
        assert data["decision"] == "approved"

        # Verify the write actually happened.
        conn = duckdb.connect(db_path)
        rows = conn.execute("SELECT * FROM t").fetchall()
        conn.close()
        assert rows == [(1,)]

    def test_original_sql_preserved_in_output(self) -> None:
        runner = CliRunner()
        envelope = _make_ask_envelope(
            effective_sql="SELECT 1 LIMIT 1000",
            original_sql="SELECT 1",
        )
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 0
        data = _parse_approve_json(result.output)
        assert data["effective_sql"] == "SELECT 1 LIMIT 1000"

    def test_adapter_error_returns_error_decision(self) -> None:
        runner = CliRunner()
        envelope = _make_ask_envelope(
            effective_sql="SELECT * FROM nonexistent_table_xyz",
        )
        result = runner.invoke(main, ["approve"], input=envelope)
        assert result.exit_code == 1
        data = _parse_approve_json(result.output)
        assert data["decision"] == "error"


class TestApproveEndToEnd:
    """Full pipeline: query → ask → approve → executed."""

    def test_cost_exceeded_then_approve(self, monkeypatch) -> None:
        """Simulate: query returns ask (cost exceeded), approve executes."""
        from dbastion.adapters import duckdb as duckdb_adapter
        from dbastion.adapters._base import CostEstimate, CostUnit

        async def _dry_run_big(self, sql):
            return CostEstimate(
                raw_value=100e9, unit=CostUnit.BYTES,
                estimated_gb=200, summary="200 GB",
            )

        monkeypatch.setattr(duckdb_adapter.DuckDBAdapter, "dry_run", _dry_run_big)

        runner = CliRunner()

        # Step 1: query returns ask
        query_result = runner.invoke(main, [
            "query", "SELECT 1 AS x", "--db", "duckdb:", "--format", "json",
        ])
        assert query_result.exit_code == 1
        query_data = json.loads(query_result.output)
        assert query_data["decision"] == "ask"

        # Step 2: approve executes (monkeypatch is only on dry_run, execute works)
        monkeypatch.undo()
        approve_result = runner.invoke(main, ["approve"], input=query_result.output)
        assert approve_result.exit_code == 0
        approve_data = _parse_approve_json(approve_result.output)
        assert approve_data["decision"] == "approved"
        assert approve_data["columns"] == ["x"]
        assert approve_data["rows"] == [{"x": 1}]

    def test_write_query_then_approve(self) -> None:
        """Write queries return ask from query, then execute via approve."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as f:
            db_path = f.name
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'alice')")
        conn.close()

        runner = CliRunner()
        db = f"duckdb:path={db_path}"

        # Step 1: query returns ask for DML
        query_result = runner.invoke(main, [
            "query", "DELETE FROM t WHERE id = 1",
            "--db", db, "--format", "json",
        ])
        assert query_result.exit_code == 0
        query_data = json.loads(query_result.output)
        assert query_data["decision"] == "ask"
        assert query_data["classification"] == "dml"

        # Step 2: approve executes the delete
        approve_result = runner.invoke(main, ["approve"], input=query_result.output)
        assert approve_result.exit_code == 0
        approve_data = _parse_approve_json(approve_result.output)
        assert approve_data["decision"] == "approved"

        # Verify the delete happened
        conn = duckdb.connect(db_path)
        rows = conn.execute("SELECT * FROM t").fetchall()
        conn.close()
        assert rows == []
