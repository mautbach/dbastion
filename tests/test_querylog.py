"""Test query logging — daily JSONL files with retention cleanup."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from dbastion.querylog import _project_slug, cleanup_old_logs, log_query


def test_project_slug_encodes_cwd():
    with patch("dbastion.querylog.os.getcwd", return_value="/Users/bach/projects/dbastion"):
        slug = _project_slug()
    assert slug == "Users-bach-projects-dbastion"


def test_log_query_creates_file(tmp_path):
    """log_query creates a daily JSONL file and appends an entry."""
    with patch("dbastion.querylog._LOG_ROOT", tmp_path), patch(
        "dbastion.querylog.os.getcwd", return_value="/test/project"
    ):
        log_query(sql="SELECT 1", effective_sql="SELECT 1 LIMIT 1000", db="duckdb")

    # Find the created file
    project_dir = tmp_path / "test-project"
    assert project_dir.exists()

    log_files = list(project_dir.glob("*.jsonl"))
    assert len(log_files) == 1

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    assert log_files[0].name == f"{today}.jsonl"

    # Verify content
    line = log_files[0].read_text().strip()
    entry = json.loads(line)
    assert entry["sql"] == "SELECT 1"
    assert entry["effective_sql"] == "SELECT 1 LIMIT 1000"
    assert entry["db"] == "duckdb"
    assert entry["blocked"] is False
    assert "ts" in entry


def test_log_query_appends_to_existing(tmp_path):
    """Multiple log calls append to the same daily file."""
    with patch("dbastion.querylog._LOG_ROOT", tmp_path), patch(
        "dbastion.querylog.os.getcwd", return_value="/test/project"
    ):
        log_query(sql="SELECT 1", effective_sql="SELECT 1")
        log_query(sql="SELECT 2", effective_sql="SELECT 2")

    project_dir = tmp_path / "test-project"
    log_files = list(project_dir.glob("*.jsonl"))
    assert len(log_files) == 1

    lines = log_files[0].read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["sql"] == "SELECT 1"
    assert json.loads(lines[1])["sql"] == "SELECT 2"


def test_log_query_full_fields(tmp_path):
    """All fields are recorded when provided."""
    with patch("dbastion.querylog._LOG_ROOT", tmp_path), patch(
        "dbastion.querylog.os.getcwd", return_value="/test/project"
    ):
        log_query(
            sql="SELECT * FROM users",
            effective_sql="SELECT * FROM users LIMIT 1000",
            db="bigquery:my-project",
            dialect="bigquery",
            tables=["users"],
            blocked=False,
            diagnostics=["Q0601"],
            cost_gb=0.5,
            cost_usd=0.003,
            duration_ms=450.0,
            labels={"tool": "dbastion"},
            dry_run=False,
        )

    project_dir = tmp_path / "test-project"
    line = list(project_dir.glob("*.jsonl"))[0].read_text().strip()
    entry = json.loads(line)
    assert entry["tables"] == ["users"]
    assert entry["cost_gb"] == 0.5
    assert entry["cost_usd"] == 0.003
    assert entry["duration_ms"] == 450.0
    assert entry["labels"] == {"tool": "dbastion"}
    assert entry["dry_run"] is False


def test_cleanup_deletes_old_files(tmp_path):
    """Files older than retention_days are deleted."""
    project_dir = tmp_path / "test-project"
    project_dir.mkdir(parents=True)

    # Create old file (40 days ago)
    old_date = (datetime.now(UTC) - timedelta(days=40)).strftime("%Y-%m-%d")
    (project_dir / f"{old_date}.jsonl").write_text('{"sql":"old"}\n')

    # Create recent file (5 days ago)
    recent_date = (datetime.now(UTC) - timedelta(days=5)).strftime("%Y-%m-%d")
    (project_dir / f"{recent_date}.jsonl").write_text('{"sql":"recent"}\n')

    with patch("dbastion.querylog._LOG_ROOT", tmp_path), patch(
        "dbastion.querylog.os.getcwd", return_value="/test/project"
    ):
        deleted = cleanup_old_logs(retention_days=30)

    assert deleted == 1
    assert not (project_dir / f"{old_date}.jsonl").exists()
    assert (project_dir / f"{recent_date}.jsonl").exists()


def test_cleanup_no_directory(tmp_path):
    """Cleanup is a no-op when log directory doesn't exist."""
    with patch("dbastion.querylog._LOG_ROOT", tmp_path), patch(
        "dbastion.querylog.os.getcwd", return_value="/nonexistent/project"
    ):
        deleted = cleanup_old_logs()
    assert deleted == 0
