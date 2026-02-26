"""Internal types for the policy engine."""

from __future__ import annotations

import enum


class StatementType(enum.Enum):
    READ = "read"
    DML = "dml"
    DDL = "ddl"
    ADMIN = "admin"      # GRANT, COPY, SET ROLE, etc.
    UNKNOWN = "unknown"  # Anything we can't classify → blocked
