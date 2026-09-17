"""Engine-neutral schema metadata for canonical Silver, current-state and Gold.

Resolution and business policies live in current_state and gold_open_opportunities.
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
CURRENT_STATE_ISSUES_SCHEMA_VERSION = 1
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
# an invented state; builders surface them in current_state_issues.
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

# Grain: one row per canonical Silver event that cannot be resolved to a
# deterministic current state. `procedure_id` is nullable only to preserve
# provenance for events without a procedure key; every other field is the
# canonical Silver provenance of that event plus a deterministic `reason`.
# Reasons distinguish genuinely undated ordering (`undated_competing_events`)
# from identity and missing-key failures.
CURRENT_STATE_ISSUES_FIELDS = (
    FieldSpec("procedure_id", "string", True),
    FieldSpec("event_id", "string", False),
    FieldSpec("source", "string", False),
    FieldSpec("source_event_type", "string", False),
    FieldSpec("reason", "string", False),
)

# Grain: one row per current-state procedure that the Gold builder has
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
