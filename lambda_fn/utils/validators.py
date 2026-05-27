"""
lambda_fn/utils/validators.py
------------------------------
Reusable validation and enrichment helpers shared across Lambda handlers.
Separated from handler.py to keep that file focused on orchestration
and to make unit testing of business logic straightforward.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

# ISO 8601 timestamp pattern (loose — accept with or without timezone)
_TS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")

REQUIRED_FIELDS: frozenset[str] = frozenset({"event_id", "event_type", "timestamp", "user_id"})

VALID_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "page_view",
        "click",
        "purchase",
        "signup",
        "error",
        "api_call",
        "session_start",
        "session_end",
        "search",
        "add_to_cart",
        "checkout",
    }
)


class ValidationError(ValueError):
    """Raised when an event payload fails schema validation."""


def validate_payload(payload: dict[str, Any]) -> None:
    """
    Validate required fields, types, and value ranges.
    Raises ValidationError describing every problem found.
    """
    errors: list[str] = []

    # ── Required fields ────────────────────────────────────────────────────
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        errors.append(f"missing required fields: {sorted(missing)}")

    # ── Field-level validation (only if present) ───────────────────────────
    if "event_id" in payload:
        if not isinstance(payload["event_id"], str) or len(payload["event_id"]) < 1:
            errors.append("event_id must be a non-empty string")

    if "event_type" in payload:
        if payload["event_type"] not in VALID_EVENT_TYPES:
            errors.append(
                f"unknown event_type '{payload['event_type']}'; "
                f"expected one of {sorted(VALID_EVENT_TYPES)}"
            )

    if "timestamp" in payload:
        ts = payload["timestamp"]
        if not isinstance(ts, str) or not _TS_PATTERN.match(ts):
            errors.append(f"timestamp '{ts}' does not match ISO 8601 format")

    if "user_id" in payload:
        if not isinstance(payload["user_id"], str) or len(payload["user_id"]) < 1:
            errors.append("user_id must be a non-empty string")

    if errors:
        raise ValidationError("; ".join(errors))


def enrich_payload(
    payload: dict[str, Any],
    sequence_number: str,
    shard_id: str = "",
) -> dict[str, Any]:
    """
    Enrich a validated raw payload with processing metadata.
    Returns a new dict — never mutates the input.
    """
    return {
        **payload,
        "_meta": {
            "kinesis_sequence": sequence_number,
            "kinesis_shard_id": shard_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "processed_by": "lambda-transformer-v1",
            "schema_version": "1.0",
        },
    }


def sanitize_string(value: str, max_length: int = 255) -> str:
    """Strip whitespace and truncate to max_length."""
    return value.strip()[:max_length]
