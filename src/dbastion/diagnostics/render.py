"""Render diagnostics for terminal (text) and agent (JSON) output."""

from __future__ import annotations

from dbastion.diagnostics.types import Applicability, Diagnostic, DiagnosticResult


def render_json(result: DiagnosticResult) -> dict:
    """Render a DiagnosticResult as a JSON-serializable dict."""
    d: dict = {
        "original_sql": result.original_sql,
        "healed_sql": result.healed_sql,
        "effective_sql": result.effective_sql,
        "blocked": result.blocked,
        "tables": result.tables,
        "applied_fixes": result.applied_fixes_summary(),
        "diagnostics": [_diagnostic_to_dict(diag) for diag in result.diagnostics],
    }
    if result.classification is not None:
        d["classification"] = result.classification
    return d


def render_text(result: DiagnosticResult) -> str:
    """Render a DiagnosticResult as human-readable text."""
    lines: list[str] = []
    for d in result.diagnostics:
        lines.append(f"{d.level.name.lower()}[{d.code}]: {d.message}")
        for note in d.notes:
            lines.append(f"  = note: {note}")
        for s in d.suggestions:
            prefix = "fix" if s.applicability == Applicability.MACHINE_APPLICABLE else "help"
            lines.append(f"  = {prefix}: {s.message}")

    if result.healed_sql is not None:
        lines.append("")
        lines.append(f"effective SQL: {result.effective_sql}")

    return "\n".join(lines)


def _diagnostic_to_dict(d: Diagnostic) -> dict:
    return {
        "level": d.level.name.lower(),
        "code": str(d.code),
        "message": d.message,
        "notes": d.notes,
    }
