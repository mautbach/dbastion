"""Tests for the diagnostic system — ported from Rust crates/core/src/diagnostics.rs."""

from dbastion.diagnostics import (
    Applicability,
    Diagnostic,
    DiagnosticResult,
    Level,
    Span,
    SpanKind,
    apply_fixes,
    codes,
)


def test_apply_single_fix():
    sql = "SELECT usename FROM users"
    diag = Diagnostic.error(
        codes.COLUMN_NOT_FOUND, "column 'usename' not found"
    ).span(Span(7, 14), "not found in 'users'").fix(
        "replace with `username`", Span(7, 14), "username"
    )

    healed = apply_fixes(sql, [diag])
    assert healed == "SELECT username FROM users"


def test_apply_multiple_fixes():
    sql = "SELECT usename, emial FROM users"
    d1 = Diagnostic.error(
        codes.COLUMN_NOT_FOUND, "column 'usename' not found"
    ).fix("replace with `username`", Span(7, 14), "username")
    d2 = Diagnostic.error(
        codes.COLUMN_NOT_FOUND, "column 'emial' not found"
    ).fix("replace with `email`", Span(16, 21), "email")

    healed = apply_fixes(sql, [d1, d2])
    assert healed == "SELECT username, email FROM users"


def test_no_fixes_returns_none():
    sql = "SELECT id FROM users"
    diag = Diagnostic.warning(
        codes.VALUE_NOT_IN_COLUMN, "value not found"
    ).suggest("did you mean 'active'?", Span(0, 5), "active")

    # MaybeIncorrect — should not be auto-applied.
    assert apply_fixes(sql, [diag]) is None


def test_overlapping_spans_bail_out():
    sql = "SELECT ab FROM t"
    d1 = Diagnostic.error(codes.COLUMN_NOT_FOUND, "err").fix(
        "fix a", Span(7, 9), "xxx"
    )
    d2 = Diagnostic.error(codes.COLUMN_NOT_FOUND, "err").fix(
        "fix b", Span(8, 10), "yyy"
    )

    # Overlapping — should return None rather than corrupt SQL.
    assert apply_fixes(sql, [d1, d2]) is None


def test_diagnostic_code_display():
    assert str(codes.COLUMN_NOT_FOUND) == "Q0102"
    assert str(codes.LIMIT_INJECTED) == "Q0601"


def test_diagnostic_result_effective_sql():
    result = DiagnosticResult(
        original_sql="SELECT usename FROM users",
        healed_sql="SELECT username FROM users",
        diagnostics=[],
        blocked=False,
    )
    assert result.effective_sql == "SELECT username FROM users"

    result_no_heal = DiagnosticResult(
        original_sql="SELECT id FROM users",
        healed_sql=None,
        diagnostics=[],
        blocked=False,
    )
    assert result_no_heal.effective_sql == "SELECT id FROM users"


def test_builder_api():
    diag = (
        Diagnostic.error(
            codes.COLUMN_NOT_FOUND,
            "column 'usename' not found in 'users'",
        )
        .span(Span(7, 14), "not found")
        .secondary_span(Span(20, 25), "table 'users'")
        .note("available columns: id, username, email, created_at, status")
        .fix("replace with `username`", Span(7, 14), "username")
    )

    assert diag.level == Level.ERROR
    assert len(diag.spans) == 2
    assert diag.spans[0].kind == SpanKind.PRIMARY
    assert diag.spans[1].kind == SpanKind.SECONDARY
    assert len(diag.notes) == 1
    assert len(diag.suggestions) == 1
    assert diag.suggestions[0].applicability == Applicability.MACHINE_APPLICABLE
