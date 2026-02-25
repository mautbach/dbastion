"""Stable, searchable error code registry.

Ranges:
- Q0001      — General (syntax errors)
- Q01xx      — Schema validation
- Q02xx      — Safety checks
- Q03xx      — Classification / access control
- Q04xx      — Cost estimation
- Q05xx      — Data warnings
- Q06xx      — Enrichment (info-level)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DiagnosticCode:
    value: int

    def __str__(self) -> str:
        return f"Q{self.value:04d}"


# General
SYNTAX_ERROR = DiagnosticCode(1)

# Schema validation (Q01xx)
TABLE_NOT_FOUND = DiagnosticCode(101)
COLUMN_NOT_FOUND = DiagnosticCode(102)
AMBIGUOUS_COLUMN = DiagnosticCode(103)

# Safety checks (Q02xx)
DELETE_WITHOUT_WHERE = DiagnosticCode(201)
MULTIPLE_STATEMENTS = DiagnosticCode(202)
UPDATE_WITHOUT_WHERE = DiagnosticCode(203)
CROSS_JOIN_NO_CONDITION = DiagnosticCode(204)
CONSTANT_CONDITION = DiagnosticCode(205)

# Classification / access control (Q03xx)
WRITE_BLOCKED = DiagnosticCode(301)
DDL_BLOCKED = DiagnosticCode(302)

# Cost estimation (Q04xx)
COST_OVER_THRESHOLD = DiagnosticCode(401)
FULL_TABLE_SCAN = DiagnosticCode(402)

# Data warnings (Q05xx)
VALUE_NOT_IN_COLUMN = DiagnosticCode(501)
TYPE_MISMATCH = DiagnosticCode(502)

# Enrichment (Q06xx)
LIMIT_INJECTED = DiagnosticCode(601)
SELECT_STAR_EXPANDED = DiagnosticCode(602)
