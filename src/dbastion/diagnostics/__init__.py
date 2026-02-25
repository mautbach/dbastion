"""Diagnostic system: types, codes, rendering, and auto-healing."""

from dbastion.diagnostics.codes import DiagnosticCode
from dbastion.diagnostics.types import (
    Applicability,
    Diagnostic,
    DiagnosticResult,
    Level,
    Span,
    SpanKind,
    SpanLabel,
    SubstitutionPart,
    Suggestion,
    apply_fixes,
)

__all__ = [
    "Applicability",
    "Diagnostic",
    "DiagnosticCode",
    "DiagnosticResult",
    "Level",
    "Span",
    "SpanKind",
    "SpanLabel",
    "Suggestion",
    "SubstitutionPart",
    "apply_fixes",
]
