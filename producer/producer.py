"""
producer.py
-----------
High-throughput Kinesis PutRecords publisher.
Targets ~5,000 events/second by batching up to 500 records per call
and running concurrent threads.

Usage:
    python producer/producer.py --stream my-events-stream --rate 5000
"""

import argparse
import json
import logging
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from producer.config import ProducerConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

# Kinesis PutRecords hard limit
MAX_BATCH_SIZE = 500


def build_event(event_type: str | None = None) -> dict[str, Any]:
    """Return a realistic application event payload."""
    event_types = ["page_view", "click", "purchase", "signup", "error", "api_call"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type or random.choice(event_types),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1, 100_000)}",
        "session_id": str(uuid.uuid4()),
        "properties": {
            "page": random.choice(["/home", "/product", "/cart", "/checkout"]),
            "duration_ms": random.randint(50, 3000),
            "value": round(random.uniform(0, 500), 2),
        },
        "metadata": {
            "producer_version": "1.0.0",
            "region": ProducerConfig.AWS_REGION,
        },
    }


def put_records_batch(
    client: Any,
    stream_name: str,
    records: list[dict],
) -> dict[str, int]:
    """
    Send a single PutRecords batch (≤500 records).
    Returns counts of successes and failures.
    """
    kinesis_records = [
        {
            "Data": json.dumps(rec).encode("utf-8"),
            # Partition by user_id for ordering per-user
            "PartitionKey": rec["user_id"],
        }
        for rec in records
    ]

    try:
        response = client.put_records(
            Records=kinesis_records,
            StreamName=stream_name,
        )
    except ClientError as exc:
        log.error("PutRecords failed: %s", exc)
        return {"sent": 0, "failed": len(records)}

    failed = response.get("FailedRecordCount", 0)
    sent = len(records) - failed

    if failed:
        log.warning(
            "Partial failure: %d/%d records not written — will be retried by Kinesis",
            failed,
            len(records),
        )

    return {"sent": sent, "failed": failed}


def producer_worker(
    stream_name: str,
    target_rps: int,
    duration_secs: int,
    thread_id: int,
    stats: dict,
    lock: threading.Lock,
) -> None:
    """Single producer thread — runs for `duration_secs` seconds."""
    client = boto3.client("kinesis", region_name=ProducerConfig.AWS_REGION)
    interval = MAX_BATCH_SIZE / target_rps  # seconds between batches for this thread
    deadline = time.monotonic() + duration_secs
    local_sent = 0
    local_failed = 0

    log.info("Thread %d started — targeting %d events/s", thread_id, target_rps)

    while time.monotonic() < deadline:
        tick = time.monotonic()
        batch = [build_event() for _ in range(MAX_BATCH_SIZE)]
        result = put_records_batch(client, stream_name, batch)
        local_sent += result["sent"]
        local_failed += result["failed"]

        elapsed = time.monotonic() - tick
        sleep_for = max(0.0, interval - elapsed)
        time.sleep(sleep_for)

    with lock:
        stats["total_sent"] += local_sent
        stats["total_failed"] += local_failed

    log.info(
        "Thread %d done — sent=%d failed=%d",
        thread_id,
        local_sent,
        local_failed,
    )


def run(stream_name: str, target_rps: int, duration_secs: int, threads: int) -> None:
    """Coordinate multiple producer threads."""
    log.info(
        "Starting producer — stream=%s rps=%d threads=%d duration=%ds",
        stream_name,
        target_rps,
        threads,
        duration_secs,
    )

    per_thread_rps = target_rps // threads
    stats: dict[str, int] = {"total_sent": 0, "total_failed": 0}
    lock = threading.Lock()
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [
            pool.submit(
                producer_worker,
                stream_name,
                per_thread_rps,
                duration_secs,
                i,
                stats,
                lock,
            )
            for i in range(threads)
        ]
        for f in as_completed(futures):
            exc = f.exception()
            if exc:
                log.error("Worker raised exception: %s", exc)

    elapsed = time.monotonic() - start
    actual_rps = stats["total_sent"] / elapsed if elapsed > 0 else 0

    log.info(
        "Producer finished — total_sent=%d total_failed=%d " "elapsed=%.1fs actual_rps=%.0f",
        stats["total_sent"],
        stats["total_failed"],
        elapsed,
        actual_rps,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Kinesis event producer")
    parser.add_argument("--stream", required=True, help="Kinesis stream name")
    parser.add_argument("--rate", type=int, default=5000, help="Target events/second")
    parser.add_argument("--duration", type=int, default=300, help="Run duration (seconds)")
    parser.add_argument("--threads", type=int, default=4, help="Parallel producer threads")
    args = parser.parse_args()

    run(
        stream_name=args.stream,
        target_rps=args.rate,
        duration_secs=args.duration,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
