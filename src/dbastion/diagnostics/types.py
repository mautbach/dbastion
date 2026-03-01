"""Rust compiler-inspired diagnostic system for SQL query analysis.

Every check in the Policy Engine produces Diagnostic values. Machine-applicable
suggestions are auto-applied before execution; all diagnostics are returned to
the caller so agents learn from corrections.

See docs/DIAGNOSTICS.md for design rationale and examples.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from dbastion.diagnostics.codes import DiagnosticCode


class Level(enum.IntEnum):
    INFO = 0
    WARNING = 1
    ERROR = 2


class Applicability(enum.Enum):
    MACHINE_APPLICABLE = "machine_applicable"
    MAYBE_INCORRECT = "maybe_incorrect"
    HAS_PLACEHOLDERS = "has_placeholders"


@dataclass(frozen=True)
class Span:
    start: int
    end: int

    def slice(self, sql: str) -> str:
        return sql[self.start : self.end]

    def __len__(self) -> int:
        return self.end - self.start

    @property
    def is_empty(self) -> bool:
        return self.start == self.end


class SpanKind(enum.Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"


@dataclass
class SpanLabel:
    span: Span
    kind: SpanKind
    label: str | None = None


@dataclass
class SubstitutionPart:
    span: Span
    replacement: str


@dataclass
class Suggestion:
    message: str
    parts: list[SubstitutionPart] = field(default_factory=list)
    applicability: Applicability = Applicability.HAS_PLACEHOLDERS


@dataclass
class Diagnostic:
    level: Level
    code: DiagnosticCode
    message: str
    spans: list[SpanLabel] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    suggestions: list[Suggestion] = field(default_factory=list)

    # -- Builder classmethods ---------------------------------------------------

    @classmethod
    def error(cls, code: DiagnosticCode, message: str) -> Diagnostic:
        return cls(level=Level.ERROR, code=code, message=message)

    @classmethod
    def warning(cls, code: DiagnosticCode, message: str) -> Diagnostic:
        return cls(level=Level.WARNING, code=code, message=message)

    @classmethod
    def info(cls, code: DiagnosticCode, message: str) -> Diagnostic:
        return cls(level=Level.INFO, code=code, message=message)

    # -- Builder chain methods --------------------------------------------------

    def span(self, span: Span, label: str) -> Diagnostic:
        self.spans.append(SpanLabel(span=span, kind=SpanKind.PRIMARY, label=label))
        return self

    def secondary_span(self, span: Span, label: str) -> Diagnostic:
        self.spans.append(SpanLabel(span=span, kind=SpanKind.SECONDARY, label=label))
        return self

    def note(self, note: str) -> Diagnostic:
        self.notes.append(note)
        return self

    def fix(self, message: str, span: Span, replacement: str) -> Diagnostic:
        self.suggestions.append(
            Suggestion(
                message=message,
                parts=[SubstitutionPart(span=span, replacement=replacement)],
                applicability=Applicability.MACHINE_APPLICABLE,
            )
        )
        return self

    def suggest(self, message: str, span: Span, replacement: str) -> Diagnostic:
        self.suggestions.append(
            Suggestion(
                message=message,
                parts=[SubstitutionPart(span=span, replacement=replacement)],
                applicability=Applicability.MAYBE_INCORRECT,
            )
        )
        return self

    def suggest_template(self, message: str) -> Diagnostic:
        self.suggestions.append(
            Suggestion(message=message, applicability=Applicability.HAS_PLACEHOLDERS)
        )
        return self

    # -- Query methods ----------------------------------------------------------

    @property
    def is_blocking(self) -> bool:
        return self.level == Level.ERROR

    def auto_fixable_suggestions(self) -> list[Suggestion]:
        return [
            s for s in self.suggestions if s.applicability == Applicability.MACHINE_APPLICABLE
        ]


@dataclass
class DiagnosticResult:
    original_sql: str
    healed_sql: str | None
    diagnostics: list[Diagnostic]
    blocked: bool
    tables: list[str] = field(default_factory=list)
    classification: str | None = None

    @property
    def effective_sql(self) -> str:
        return self.healed_sql if self.healed_sql is not None else self.original_sql

    def applied_fixes_summary(self) -> list[str]:
        return [
            f"{d.code}: {s.message}"
            for d in self.diagnostics
            for s in d.suggestions
            if s.applicability == Applicability.MACHINE_APPLICABLE
        ]

    @property
    def max_level(self) -> Level | None:
        if not self.diagnostics:
            return None
        return max(d.level for d in self.diagnostics)


def apply_fixes(sql: str, diagnostics: list[Diagnostic]) -> str | None:
    """Apply all MachineApplicable suggestions to the SQL string.

    Suggestions are applied in reverse byte-offset order so that earlier
    spans remain valid after later replacements. Returns None if no fixes
    were applied.
    """
    parts: list[SubstitutionPart] = [
        part
        for d in diagnostics
        for s in d.auto_fixable_suggestions()
        for part in s.parts
    ]

    if not parts:
        return None

    # Sort by start offset descending — apply from end to start.
    parts.sort(key=lambda p: p.span.start, reverse=True)

    # Check for overlapping spans.
    for i in range(len(parts) - 1):
        # parts[i] has later start (sorted descending).
        if parts[i + 1].span.end > parts[i].span.start:
            return None

    result = sql
    for part in parts:
        result = result[: part.span.start] + part.replacement + result[part.span.end :]

    return result
