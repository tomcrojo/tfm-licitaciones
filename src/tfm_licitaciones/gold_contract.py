"""Engine-neutral contracts for the canonical Silver -> Spark -> Gold boundary.

This module contains schema metadata only. It deliberately does not implement
current-state selection or Gold business logic: those semantics are documented
and must be implemented in later PRs once the remaining ordering decisions are
resolved.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldSpec:
    """Describe one stable field without importing an execution engine."""

    name: str
    logical_type: str
    nullable: bool = True


CURRENT_STATE_DATASET = "current_state"
CURRENT_STATE_ISSUES_DATASET = "current_state_issues"
GOLD_OPEN_OPPORTUNITIES_DATASET = "open_opportunities"

CURRENT_STATE_SCHEMA_VERSION = 1
GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION = 1

# Canonical Silver contract as consumed at the Spark boundary. Field order and
# logical types must remain in parity with models.PROCUREMENT_EVENT_SCHEMA;
# nullability additionally freezes the invariants enforced by ProcurementEvent.
CANONICAL_SILVER_FIELDS = (
    FieldSpec("event_id", "string", False),
    FieldSpec("procedure_id", "string"),
    FieldSpec("source", "string", False),
    FieldSpec("source_event_type", "string", False),
    FieldSpec("buyer_id", "string"),
    FieldSpec("buyer_name", "string"),
    FieldSpec("title", "string"),
    FieldSpec("description", "string"),
    FieldSpec("cpv_codes", "array<string>", False),
    FieldSpec("estimated_value", "decimal(20,2)"),
    FieldSpec("awarded_value", "decimal(20,2)"),
    FieldSpec("currency", "string"),
    FieldSpec("publication_date", "date"),
    FieldSpec("source_updated_at", "timestamp"),
    FieldSpec("deadline", "timestamp"),
    FieldSpec("status", "string"),
    FieldSpec("nuts_code", "string"),
    FieldSpec("country", "string"),
    FieldSpec("source_url", "string"),
    FieldSpec("ingested_at", "timestamp", False),
)

# Grain: one row per source procedure_id only after the procedure can be
# resolved deterministically. Rows that cannot be resolved safely never receive
# an invented state; future builders must surface them in current_state_issues.
CURRENT_STATE_FIELDS = (
    FieldSpec("procedure_id", "string", False),
    FieldSpec("event_id", "string", False),
    FieldSpec("source", "string", False),
    FieldSpec("source_event_type", "string", False),
    FieldSpec("buyer_id", "string"),
    FieldSpec("buyer_name", "string"),
    FieldSpec("title", "string"),
    FieldSpec("description", "string"),
    FieldSpec("cpv_codes", "array<string>", False),
    FieldSpec("estimated_value", "decimal(20,2)"),
    FieldSpec("awarded_value", "decimal(20,2)"),
    FieldSpec("currency", "string"),
    FieldSpec("publication_date", "date"),
    FieldSpec("source_updated_at", "timestamp"),
    FieldSpec("deadline", "timestamp"),
    FieldSpec("status", "string"),
    FieldSpec("nuts_code", "string"),
    FieldSpec("country", "string"),
    FieldSpec("source_url", "string"),
    FieldSpec("ingested_at", "timestamp", False),
    FieldSpec("is_deleted", "boolean", False),
)

# Grain: one row per current-state procedure that a future Gold builder has
# classified as an open/actionable opportunity. No ranking/model fields are
# frozen here because their semantics are not implemented yet.
GOLD_OPEN_OPPORTUNITIES_FIELDS = (
    FieldSpec("procedure_id", "string", False),
    FieldSpec("event_id", "string", False),
    FieldSpec("source", "string", False),
    FieldSpec("buyer_id", "string"),
    FieldSpec("buyer_name", "string"),
    FieldSpec("title", "string"),
    FieldSpec("description", "string"),
    FieldSpec("cpv_codes", "array<string>", False),
    FieldSpec("estimated_value", "decimal(20,2)"),
    FieldSpec("awarded_value", "decimal(20,2)"),
    FieldSpec("currency", "string"),
    FieldSpec("publication_date", "date"),
    FieldSpec("source_updated_at", "timestamp"),
    FieldSpec("deadline", "timestamp"),
    FieldSpec("status", "string"),
    FieldSpec("nuts_code", "string"),
    FieldSpec("country", "string"),
    FieldSpec("source_url", "string"),
    FieldSpec("ingested_at", "timestamp", False),
)
