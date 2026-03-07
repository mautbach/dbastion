"""The `approve` command: execute a query that was blocked by cost or write policy.

Reads an ask-envelope from stdin (piped from `dbastion query`),
shows a summary, and prompts for confirmation before executing.

Usage:
    dbastion query "SELECT ..." --db prod | dbastion approve
"""

from __future__ import annotations

import asyncio
import json
import sys

import click

from dbastion.adapters._base import AdapterError
from dbastion.cli._shared import execute_and_emit, parse_db


def _read_envelope() -> dict:
    """Read the ask-envelope JSON from stdin."""
    if sys.stdin.isatty():
        raise click.UsageError(
            "No envelope on stdin. Usage:\n"
            "  dbastion query \"SELECT ...\" --db prod | dbastion approve"
        )
    raw = sys.stdin.read().strip()
    if not raw:
        raise click.UsageError("stdin was empty — no envelope to approve.")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise click.UsageError(f"Invalid JSON on stdin: {e}") from e


def _has_tty() -> bool:
    """Check if /dev/tty is available (interactive terminal)."""
    try:
        with open("/dev/tty"):
            return True
    except OSError:
        return False


def _prompt_tty(message: str) -> str:
    """Prompt on /dev/tty (bypasses stdin which is the pipe)."""
    with open("/dev/tty") as tty, open("/dev/tty", "w") as tty_out:
        tty_out.write(message)
        tty_out.flush()
        return tty.readline().strip()


@click.command()
def approve() -> None:
    """Approve and execute a blocked query.

    Reads the JSON envelope from stdin (piped from `dbastion query`),
    shows what will be executed, and asks for confirmation.

    When a TTY is available, prompts interactively. When no TTY is
    available (e.g. agent harness like Claude Code), executes directly —
    the harness's own permission prompt serves as human approval.

    \b
    Example:
        dbastion query "SELECT ..." --db prod | dbastion approve
    """
    envelope = _read_envelope()

    # Only accept ask decisions.
    decision = envelope.get("decision")
    if decision == "deny":
        click.echo(json.dumps({
            "error": "This query was denied by policy and cannot be approved. "
                     "Fix the SQL and retry.",
        }, indent=2))
        raise SystemExit(1)
    if decision == "allow":
        click.echo(json.dumps({
            "error": "This query was already allowed — no approval needed.",
        }, indent=2))
        raise SystemExit(0)
    if decision != "ask":
        click.echo(json.dumps({
            "error": f"Unexpected decision '{decision}' — expected 'ask'.",
        }, indent=2))
        raise SystemExit(1)

    # Validate required fields.
    db_name = envelope.get("db")
    if not db_name:
        click.echo(json.dumps({
            "error": "Envelope missing 'db' field — cannot determine connection.",
        }, indent=2))
        raise SystemExit(1)

    effective_sql = envelope.get("effective_sql")
    if not effective_sql:
        click.echo(json.dumps({
            "error": "Envelope missing 'effective_sql' field.",
        }, indent=2))
        raise SystemExit(1)

    # Build summary for the human.
    estimate = envelope.get("estimate", {})
    cost_error = envelope.get("cost_error")
    classification = envelope.get("classification", "???")

    summary_lines = [
        f"  SQL:    {effective_sql}",
        f"  DB:     {db_name}",
        f"  Type:   {classification}",
    ]
    if estimate:
        summary_lines.append(f"  Cost:   {estimate.get('summary', 'unknown')}")
    if cost_error:
        summary_lines.append(f"  Reason: {cost_error}")

    summary = "\n".join(summary_lines)

    if _has_tty():
        # Interactive terminal: prompt the human directly.
        answer = _prompt_tty(f"\n{summary}\n\nApprove? [y/N] ")
        if answer.lower() not in ("y", "yes"):
            click.echo(json.dumps({"decision": "rejected", "reason": "user declined"}, indent=2))
            raise SystemExit(1)
    else:
        # No TTY (agent harness like Claude Code) — the harness's own
        # permission prompt serves as the human approval.
        click.echo(f"\n{summary}\n", err=True)

    # Execute via shared path.
    try:
        config = parse_db(db_name)
        exit_code = asyncio.run(execute_and_emit(
            config,
            effective_sql,
            original_sql=envelope.get("original_sql"),
            tables=envelope.get("tables"),
        ))
    except AdapterError as e:
        click.echo(json.dumps({"decision": "error", "status": "error", "error": str(e)}, indent=2))
        raise SystemExit(1) from e

    if exit_code != 0:
        raise SystemExit(exit_code)
