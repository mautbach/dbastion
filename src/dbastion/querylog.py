"""Query logging — daily JSONL files per project, with automatic retention cleanup."""

from __future__ import annotations

import contextlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

DEFAULT_RETENTION_DAYS = 30
_LOG_ROOT = Path.home() / ".dbastion" / "logs"


def _project_slug() -> str:
    """Encode cwd into a directory-safe slug, matching Claude Code's convention."""
    cwd = os.getcwd()
    return cwd.replace("/", "-").lstrip("-")


def _log_dir() -> Path:
    """Return the log directory for the current project."""
    return _LOG_ROOT / _project_slug()


def _today_file() -> Path:
    """Return today's log file path."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    return _log_dir() / f"{today}.jsonl"


def log_query(
    *,
    sql: str,
    effective_sql: str,
    db: str | None = None,
    dialect: str | None = None,
    tables: list[str] | None = None,
    blocked: bool = False,
    diagnostics: list[str] | None = None,
    cost_gb: float | None = None,
    cost_usd: float | None = None,
    duration_ms: float | None = None,
    labels: dict[str, str] | None = None,
    dry_run: bool = False,
) -> None:
    """Append a query log entry to today's JSONL file."""
    entry = {
        "ts": datetime.now(UTC).isoformat(),
        "db": db,
        "dialect": dialect,
        "sql": sql,
        "effective_sql": effective_sql,
        "tables": tables or [],
        "blocked": blocked,
        "diagnostics": diagnostics or [],
        "dry_run": dry_run,
        "cost_gb": cost_gb,
        "cost_usd": cost_usd,
        "duration_ms": duration_ms,
        "labels": labels,
    }

    log_file = _today_file()
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def cleanup_old_logs(*, retention_days: int = DEFAULT_RETENTION_DAYS) -> int:
    """Delete log files older than retention_days. Returns count of deleted files."""
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    deleted = 0

    log_dir = _log_dir()
    if not log_dir.exists():
        return 0

    for log_file in log_dir.glob("*.jsonl"):
        # Parse date from filename (YYYY-MM-DD.jsonl)
        try:
            file_date = datetime.strptime(log_file.stem, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            continue
        if file_date < cutoff:
            log_file.unlink()
            deleted += 1

    # Remove empty project directories
    with contextlib.suppress(OSError):
        log_dir.rmdir()

    return deleted
