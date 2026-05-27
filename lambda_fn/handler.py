"""
handler.py
----------
AWS Lambda function triggered by a Kinesis event source mapping.

Responsibilities:
  1. Decode and validate each Kinesis record.
  2. Skip duplicates via DynamoDB idempotency keys.
  3. Transform the payload (enrich, validate schema).
  4. Forward transformed records to Kinesis Firehose.
  5. On unrecoverable errors, raise so the ESM routes to the SQS DLQ.

Environment variables (set by Terraform):
  FIREHOSE_DELIVERY_STREAM  — target delivery stream name
  IDEMPOTENCY_TABLE         — DynamoDB table for dedup keys
  DLQ_URL                   — SQS DLQ URL (used by ESM automatically)
  AWS_REGION
"""

import base64
import json
import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

from lambda_fn.idempotency import IdempotencyStore

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

FIREHOSE_STREAM = os.environ["FIREHOSE_DELIVERY_STREAM"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

_firehose = boto3.client("firehose", region_name=AWS_REGION)
_idempotency = IdempotencyStore(
    table_name=os.environ["IDEMPOTENCY_TABLE"],
    region=AWS_REGION,
)

# Firehose PutRecordBatch hard limit
FIREHOSE_BATCH_LIMIT = 500


def decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    """Base64-decode and JSON-parse a Kinesis record's Data field."""
    raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
    return json.loads(raw)


def transform(payload: dict[str, Any], sequence_number: str) -> dict[str, Any]:
    """
    Enrich the raw event before forwarding downstream.
    Add processing metadata; validate required fields.
    """
    required = {"event_id", "event_type", "timestamp", "user_id"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"Payload missing required fields: {missing}")

    return {
        **payload,
        "_meta": {
            "kinesis_sequence": sequence_number,
            "processed_by": "lambda-transformer-v1",
        },
    }


def flush_to_firehose(records: list[dict[str, Any]]) -> None:
    """
    Send a batch of transformed records to Kinesis Firehose.
    Raises on any partial failure so the ESM can retry/DLQ.
    """
    firehose_records = [
        {"Data": (json.dumps(rec) + "\n").encode("utf-8")}
        for rec in records
    ]

    # Firehose allows max 500 records per PutRecordBatch
    for i in range(0, len(firehose_records), FIREHOSE_BATCH_LIMIT):
        chunk = firehose_records[i : i + FIREHOSE_BATCH_LIMIT]
        try:
            resp = _firehose.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM,
                Records=chunk,
            )
        except ClientError as exc:
            log.error("Firehose PutRecordBatch error: %s", exc)
            raise

        failed = resp.get("FailedPutCount", 0)
        if failed:
            log.error(
                "Firehose partial failure: %d/%d records rejected",
                failed,
                len(chunk),
            )
            raise RuntimeError(f"Firehose rejected {failed} records — triggering retry")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda entry point.

    The Kinesis ESM passes batches of records. We process each record
    individually so that a single bad record (after max retries) is
    bisected out and routed to the SQS DLQ rather than blocking the shard.
    """
    records = event.get("Records", [])
    log.info("Received %d Kinesis records", len(records))

    transformed: list[dict[str, Any]] = []
    skipped_duplicates = 0
    errors = 0

    for record in records:
        seq = record["kinesis"]["sequenceNumber"]
        try:
            # ── Idempotency check ──────────────────────────────────────────
            if _idempotency.is_duplicate(seq):
                log.debug("Duplicate sequence %s — skipping", seq)
                skipped_duplicates += 1
                continue

            # ── Decode ─────────────────────────────────────────────────────
            payload = decode_kinesis_record(record)

            # ── Transform ──────────────────────────────────────────────────
            enriched = transform(payload, seq)
            transformed.append(enriched)

            # ── Mark processed ─────────────────────────────────────────────
            _idempotency.mark_processed(seq)

        except (ValueError, json.JSONDecodeError) as exc:
            # Non-retryable: malformed record — log and skip (don't block shard)
            log.error("Skipping malformed record seq=%s: %s", seq, exc)
            errors += 1

        except Exception as exc:
            # Retryable: propagate so ESM retries, then routes to DLQ
            log.error("Unhandled error on seq=%s: %s", seq, exc)
            raise

    # ── Forward to Firehose ────────────────────────────────────────────────
    if transformed:
        flush_to_firehose(transformed)

    log.info(
        "Batch complete — processed=%d duplicates_skipped=%d errors=%d",
        len(transformed),
        skipped_duplicates,
        errors,
    )

    return {
        "statusCode": 200,
        "processed": len(transformed),
        "skipped": skipped_duplicates,
        "errors": errors,
    }
