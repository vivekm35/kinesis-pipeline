"""
scripts/shard_monitor.py
------------------------
Real-time CLI dashboard showing per-shard throughput, iterator lag,
and error rates. Helps diagnose hot shards and throttling issues.

Usage:
    python scripts/shard_monitor.py --stream kinesis-pipeline-dev-events
    python scripts/shard_monitor.py --stream my-stream --interval 5
    python scripts/shard_monitor.py --endpoint http://localhost:4566 --stream my-stream
"""

import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ANSI colours
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"
CLEAR = "\033[H\033[J"


def get_shard_metrics(
    cw: Any,
    stream_name: str,
    shard_id: str,
    period: int,
) -> dict[str, float]:
    """Fetch IncomingRecords and WriteProvisionedThroughputExceeded for one shard."""
    now = datetime.now(timezone.utc)
    metrics = {}

    for metric in ("IncomingRecords", "WriteProvisionedThroughputExceeded"):
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/Kinesis",
                MetricName=metric,
                Dimensions=[
                    {"Name": "StreamName", "Value": stream_name},
                    {"Name": "ShardId", "Value": shard_id},
                ],
                StartTime=datetime.fromtimestamp(now.timestamp() - period * 2, tz=timezone.utc),
                EndTime=now,
                Period=period,
                Statistics=["Sum"],
            )
            points = resp.get("Datapoints", [])
            metrics[metric] = points[-1]["Sum"] if points else 0.0
        except ClientError:
            metrics[metric] = 0.0

    return metrics


def describe_shards(kinesis: Any, stream_name: str) -> list[dict]:
    """Return list of open (active) shards."""
    shards = []
    kwargs: dict = {"StreamName": stream_name, "MaxResults": 100}
    while True:
        resp = kinesis.list_shards(**kwargs)
        for s in resp.get("Shards", []):
            # Only open shards have no EndingSequenceNumber
            if "EndingSequenceNumber" not in s.get("SequenceNumberRange", {}):
                shards.append(s)
        token = resp.get("NextToken")
        if not token:
            break
        kwargs = {"NextToken": token, "MaxResults": 100}
    return shards


def bar(value: float, maximum: float, width: int = 20) -> str:
    """Render a simple ASCII progress bar."""
    filled = int(width * min(value / maximum, 1.0)) if maximum > 0 else 0
    pct = min(value / maximum * 100, 100) if maximum > 0 else 0
    colour = GREEN if pct < 60 else (YELLOW if pct < 85 else RED)
    return f"{colour}{'█' * filled}{'░' * (width - filled)}{RESET} {pct:5.1f}%"


def render_dashboard(
    stream_name: str,
    shards: list[dict],
    shard_metrics: dict[str, dict],
    interval: int,
) -> None:
    """Print dashboard to terminal."""
    print(CLEAR, end="")

    print("─" * 80)
    print(f"{'Shard':<20} {'Records/s':>10}  {'Utilisation (1 MB/s cap)':30}  {'Throttles':>10}")
    print("─" * 80)

    # Each shard can handle 1,000 records/s write
    SHARD_RECORD_CAP = 1_000

    for shard in sorted(shards, key=lambda s: s["ShardId"]):
        sid = shard["ShardId"]
        m = shard_metrics.get(sid, {})
        rps = m.get("IncomingRecords", 0) / interval
        throttles = m.get("WriteProvisionedThroughputExceeded", 0)
        utilisation_bar = bar(rps, SHARD_RECORD_CAP)
        throttle_str = f"{RED}{throttles:.0f}{RESET}" if throttles > 0 else f"{GREEN}0{RESET}"
        print(f"{sid:<20} {rps:>10,.1f}  {utilisation_bar}  {throttle_str:>10}")

    print("─" * 80)
    print(f"Press {BOLD}Ctrl+C{RESET} to exit  ·  interval={interval}s")


def run(stream_name: str, interval: int, endpoint: str | None) -> None:
    kwargs: dict = {"region_name": AWS_REGION}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    kinesis = boto3.client("kinesis", **kwargs)
    cw = boto3.client("cloudwatch", **kwargs)

    print(f"Connecting to stream '{stream_name}'...")
    shards = describe_shards(kinesis, stream_name)
    if not shards:
        print(f"No active shards found for stream '{stream_name}'")
        return

    print(f"Found {len(shards)} active shards. Starting monitor (interval={interval}s)...")
    time.sleep(1)

    try:
        while True:
            shards = describe_shards(kinesis, stream_name)
            shard_metrics = {
                s["ShardId"]: get_shard_metrics(cw, stream_name, s["ShardId"], interval)
                for s in shards
            }
            render_dashboard(stream_name, shards, shard_metrics, interval)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time Kinesis shard monitor")
    parser.add_argument("--stream", required=True, help="Kinesis stream name")
    parser.add_argument("--interval", type=int, default=10, help="Refresh interval (seconds)")
    parser.add_argument("--endpoint", default=None, help="AWS endpoint URL (LocalStack)")
    args = parser.parse_args()
    run(args.stream, args.interval, args.endpoint)


if __name__ == "__main__":
    main()
