"""
handler.py
----------
AWS Lambda function triggered by a Kinesis event source mapping.
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

# Clients initialised lazily so moto mocks are active when first called
_firehose = None
_idempotency = None

FIREHOSE_BATCH_LIMIT = 500


def _get_firehose():
    global _firehose
    if _firehose is None:
        _firehose = boto3.client("firehose", region_name=AWS_REGION)
    return _firehose


def _get_idempotency():
    global _idempotency
    if _idempotency is None:
        _idempotency = IdempotencyStore(
            table_name=os.environ["IDEMPOTENCY_TABLE"],
            region=AWS_REGION,
        )
    return _idempotency


def decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    """Base64-decode and JSON-parse a Kinesis record's Data field."""
    raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
    return json.loads(raw)


def transform(payload: dict[str, Any], sequence_number: str) -> dict[str, Any]:
    """Enrich the raw event before forwarding downstream."""
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
    """Send a batch of transformed records to Kinesis Firehose."""
    newline = chr(10)
    firehose_records = [{"Data": (json.dumps(rec) + newline).encode("utf-8")} for rec in records]
    for i in range(0, len(firehose_records), FIREHOSE_BATCH_LIMIT):
        chunk = firehose_records[i : i + FIREHOSE_BATCH_LIMIT]
        try:
            resp = _get_firehose().put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM,
                Records=chunk,
            )
        except ClientError as exc:
            log.error("Firehose PutRecordBatch error: %s", exc)
            raise
        failed = resp.get("FailedPutCount", 0)
        if failed:
            log.error("Firehose partial failure: %d/%d records rejected", failed, len(chunk))
            raise RuntimeError(f"Firehose rejected {failed} records - triggering retry")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Main Lambda entry point."""
    records = event.get("Records", [])
    log.info("Received %d Kinesis records", len(records))
    transformed: list[dict[str, Any]] = []
    skipped_duplicates = 0
    errors = 0
    for record in records:
        seq = record["kinesis"]["sequenceNumber"]
        try:
            if _get_idempotency().is_duplicate(seq):
                log.debug("Duplicate sequence %s - skipping", seq)
                skipped_duplicates += 1
                continue
            payload = decode_kinesis_record(record)
            enriched = transform(payload, seq)
            transformed.append(enriched)
            _get_idempotency().mark_processed(seq)
        except (ValueError, json.JSONDecodeError) as exc:
            log.error("Skipping malformed record seq=%s: %s", seq, exc)
            errors += 1
        except Exception as exc:
            log.error("Unhandled error on seq=%s: %s", seq, exc)
            raise
    if transformed:
        flush_to_firehose(transformed)
    log.info(
        "Batch complete - processed=%d duplicates_skipped=%d errors=%d",
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
