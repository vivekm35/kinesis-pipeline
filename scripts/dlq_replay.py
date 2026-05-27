"""
scripts/dlq_replay.py
---------------------
Replays failed records from the SQS Dead-Letter Queue back into
Kinesis Data Streams for reprocessing.

When to use:
  - Lambda bug is fixed and you want to reprocess records that ended up in DLQ
  - Downstream service (Firehose / Redshift) was down and records piled up
  - After any manual investigation confirms records are safe to replay

Usage:
    # Dry-run first — preview what would be replayed
    python scripts/dlq_replay.py --dry-run

    # Replay up to 1000 messages
    python scripts/dlq_replay.py --max-messages 1000

    # Replay to a different stream (e.g. staging)
    python scripts/dlq_replay.py --target-stream kinesis-pipeline-staging-events

    # LocalStack
    python scripts/dlq_replay.py --endpoint http://localhost:4566 --dry-run

Safety:
    - Idempotency keys in Lambda will automatically deduplicate any records
      that were already successfully processed before hitting the DLQ.
    - Always do a --dry-run first to inspect the payload and error context.
"""

import argparse
import json
import logging
import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DLQ_URL = os.environ.get("DLQ_URL", "")
KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME", "")

# Kinesis PutRecords max batch
BATCH_SIZE = 500


def build_clients(endpoint: str | None) -> tuple[Any, Any]:
    kwargs = {"region_name": AWS_REGION}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return (
        boto3.client("sqs", **kwargs),
        boto3.client("kinesis", **kwargs),
    )


def receive_messages(sqs: Any, dlq_url: str, max_msgs: int) -> list[dict]:
    """Pull all available messages from DLQ (up to max_msgs)."""
    messages = []
    log.info("Draining DLQ: %s", dlq_url)

    while len(messages) < max_msgs:
        batch_limit = min(10, max_msgs - len(messages))
        resp = sqs.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=batch_limit,
            WaitTimeSeconds=2,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )
        batch = resp.get("Messages", [])
        if not batch:
            break
        messages.extend(batch)
        log.info("  Received %d messages (total so far: %d)", len(batch), len(messages))

    return messages


def extract_kinesis_payload(sqs_message: dict) -> dict | None:
    """
    SQS DLQ messages from Lambda ESM wrap the original Kinesis record.
    Extract the actual event payload for replay.
    """
    try:
        body = json.loads(sqs_message["Body"])
        # ESM DLQ format: body contains the Kinesis record(s)
        if "requestContext" in body:
            # Standard ESM DLQ wrapper
            records = body.get("requestPayload", {}).get("Records", [])
            if records:
                return records[0].get("kinesis", {})
        # Fallback: body IS the payload
        return body
    except (json.JSONDecodeError, KeyError):
        log.warning("Could not parse DLQ message body: %s", sqs_message.get("MessageId"))
        return None


def replay_to_kinesis(
    kinesis: Any,
    stream_name: str,
    payloads: list[dict],
    dry_run: bool,
) -> dict[str, int]:
    """Batch-put records back into Kinesis. Returns sent/failed counts."""
    if dry_run:
        log.info("[DRY RUN] Would replay %d records to %s", len(payloads), stream_name)
        for i, p in enumerate(payloads[:5]):
            log.info("  Sample %d: %s", i + 1, json.dumps(p)[:200])
        if len(payloads) > 5:
            log.info("  ... and %d more", len(payloads) - 5)
        return {"sent": 0, "failed": 0, "dry_run": len(payloads)}

    total_sent = 0
    total_failed = 0

    for i in range(0, len(payloads), BATCH_SIZE):
        chunk = payloads[i : i + BATCH_SIZE]
        records = [
            {
                "Data": json.dumps(p).encode("utf-8") if isinstance(p, dict) else p,
                "PartitionKey": (
                    str(p.get("user_id", f"replay-{i}")) if isinstance(p, dict) else f"replay-{i}"
                ),
            }
            for p in chunk
        ]
        try:
            resp = kinesis.put_records(Records=records, StreamName=stream_name)
            failed = resp.get("FailedRecordCount", 0)
            total_sent += len(chunk) - failed
            total_failed += failed
            log.info(
                "Batch %d/%d — sent=%d failed=%d",
                i // BATCH_SIZE + 1,
                (len(payloads) + BATCH_SIZE - 1) // BATCH_SIZE,
                len(chunk) - failed,
                failed,
            )
        except ClientError as exc:
            log.error("PutRecords error: %s", exc)
            total_failed += len(chunk)

        time.sleep(0.1)  # avoid shard throttle

    return {"sent": total_sent, "failed": total_failed}


def delete_replayed_messages(sqs: Any, dlq_url: str, messages: list[dict]) -> None:
    """Delete successfully replayed messages from DLQ in batches of 10."""
    for i in range(0, len(messages), 10):
        batch = messages[i : i + 10]
        entries = [{"Id": str(j), "ReceiptHandle": m["ReceiptHandle"]} for j, m in enumerate(batch)]
        sqs.delete_message_batch(QueueUrl=dlq_url, Entries=entries)
    log.info("Deleted %d messages from DLQ", len(messages))


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay DLQ records into Kinesis")
    parser.add_argument("--dlq-url", default=DLQ_URL, help="SQS DLQ URL")
    parser.add_argument("--target-stream", default=KINESIS_STREAM, help="Kinesis stream name")
    parser.add_argument("--max-messages", type=int, default=500, help="Max records to replay")
    parser.add_argument("--dry-run", action="store_true", help="Preview without replaying")
    parser.add_argument("--endpoint", default=None, help="AWS endpoint URL (LocalStack)")
    args = parser.parse_args()

    if not args.dlq_url:
        parser.error("--dlq-url is required (or set DLQ_URL env var)")
    if not args.target_stream:
        parser.error("--target-stream is required (or set KINESIS_STREAM_NAME env var)")

    sqs, kinesis = build_clients(args.endpoint)

    messages = receive_messages(sqs, args.dlq_url, args.max_messages)
    if not messages:
        log.info("DLQ is empty — nothing to replay")
        return

    log.info("Found %d messages in DLQ", len(messages))

    payloads = [p for m in messages if (p := extract_kinesis_payload(m)) is not None]
    log.info("Extracted %d valid payloads for replay", len(payloads))

    result = replay_to_kinesis(kinesis, args.target_stream, payloads, args.dry_run)

    if not args.dry_run and result["sent"] > 0:
        # Only delete messages we successfully put back into Kinesis
        replayed_messages = messages[: result["sent"]]
        delete_replayed_messages(sqs, args.dlq_url, replayed_messages)

    log.info(
        "Replay complete — sent=%d failed=%d dry_run=%s",
        result.get("sent", 0),
        result.get("failed", 0),
        args.dry_run,
    )


if __name__ == "__main__":
    main()
