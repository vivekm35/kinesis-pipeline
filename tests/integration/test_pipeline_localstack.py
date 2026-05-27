"""
tests/integration/test_pipeline_localstack.py
----------------------------------------------
End-to-end integration tests against a running LocalStack instance.

Prerequisites:
    docker compose -f docker/docker-compose.yml up -d
    # Wait ~15s for LocalStack to be healthy and seeded

Run:
    pytest tests/integration/ -v -s \
        --localstack-endpoint http://localhost:4566

These tests exercise the full path:
  producer.put_records_batch → Kinesis stream
  lambda_fn.handler → reads from Kinesis (simulated via direct invoke)
  Verifies DynamoDB idempotency table entries
  Verifies S3 landing zone objects
"""

import base64
import json
import os
import time
import uuid
import pytest
import boto3

ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"
STREAM = "kinesis-pipeline-dev-events"
FIREHOSE = "kinesis-pipeline-dev-events-firehose"
BUCKET = "kinesis-pipeline-dev-landing"
DDB_TABLE = "kinesis-pipeline-dev-idempotency"

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = REGION
os.environ["FIREHOSE_DELIVERY_STREAM"] = FIREHOSE
os.environ["IDEMPOTENCY_TABLE"] = DDB_TABLE
_b = f"http://sqs.{REGION}.localhost.localstack.cloud:4566"
os.environ["DLQ_URL"] = f"{_b}/000000000000/kinesis-pipeline-dev-events-dlq"


def client(service: str):
    return boto3.client(service, region_name=REGION, endpoint_url=ENDPOINT)


def is_localstack_available() -> bool:
    try:
        import urllib.request

        urllib.request.urlopen(f"{ENDPOINT}/_localstack/health", timeout=2)
        return True
    except Exception:
        return False


localstack_only = pytest.mark.skipif(
    not is_localstack_available(),
    reason="LocalStack not running — start with: docker compose -f docker/docker-compose.yml up -d",
)


@localstack_only
class TestKinesisProducer:
    """Verify the producer can write to LocalStack Kinesis."""

    def test_put_records_to_stream(self):
        from producer.producer import build_event, put_records_batch

        kinesis = client("kinesis")

        events = [build_event() for _ in range(10)]
        result = put_records_batch(kinesis, STREAM, events)

        assert result["sent"] == 10
        assert result["failed"] == 0

    def test_events_are_readable_from_shard(self):
        from producer.producer import build_event, put_records_batch

        kinesis = client("kinesis")

        # Put a uniquely-tagged event
        tag = str(uuid.uuid4())
        event = build_event(event_type="api_call")
        event["_test_tag"] = tag
        put_records_batch(kinesis, STREAM, [event])

        # Read it back via GetRecords
        resp = kinesis.describe_stream_summary(StreamName=STREAM)
        shard_count = resp["StreamDescriptionSummary"]["OpenShardCount"]
        assert shard_count > 0

        shards = kinesis.list_shards(StreamName=STREAM)["Shards"]
        iterator = kinesis.get_shard_iterator(
            StreamName=STREAM,
            ShardId=shards[0]["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        found = False
        for _ in range(10):
            records_resp = kinesis.get_records(ShardIterator=iterator, Limit=100)
            for r in records_resp.get("Records", []):
                data = json.loads(r["Data"].decode())
                if data.get("_test_tag") == tag:
                    found = True
                    break
            if found:
                break
            iterator = records_resp.get("NextShardIterator", "")
            if not iterator:
                break
            time.sleep(0.5)

        assert found, f"Event with tag {tag} not found in stream"


@localstack_only
class TestLambdaHandler:
    """Invoke the Lambda handler directly against LocalStack AWS services."""

    def _make_kinesis_event(self, payload: dict, sequence: str) -> dict:
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        return {
            "Records": [
                {
                    "kinesis": {
                        "sequenceNumber": sequence,
                        "data": encoded,
                        "partitionKey": payload.get("user_id", "test"),
                        "approximateArrivalTimestamp": time.time(),
                    },
                    "eventSource": "aws:kinesis",
                    "awsRegion": REGION,
                }
            ]
        }

    def test_handler_processes_valid_event(self):
        import importlib
        import lambda_fn.handler as h

        importlib.reload(h)

        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view",
            "timestamp": "2024-05-27T14:00:00Z",
            "user_id": f"user_{uuid.uuid4().hex[:8]}",
        }
        seq = f"integration-seq-{uuid.uuid4().hex}"
        event = self._make_kinesis_event(payload, seq)

        result = h.lambda_handler(event, None)
        assert result["statusCode"] == 200
        assert result["processed"] == 1
        assert result["skipped"] == 0

    def test_handler_deduplicates_same_sequence(self):
        import importlib
        import lambda_fn.handler as h

        importlib.reload(h)

        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "click",
            "timestamp": "2024-05-27T14:00:00Z",
            "user_id": f"user_{uuid.uuid4().hex[:8]}",
        }
        seq = f"dedup-seq-{uuid.uuid4().hex}"
        event = self._make_kinesis_event(payload, seq)

        # First invocation — should process
        result1 = h.lambda_handler(event, None)
        assert result1["processed"] == 1

        # Second invocation — same sequence number should be skipped
        result2 = h.lambda_handler(event, None)
        assert result2["skipped"] == 1
        assert result2["processed"] == 0

    def test_idempotency_key_written_to_dynamodb(self):
        import importlib
        import lambda_fn.handler as h

        importlib.reload(h)

        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "signup",
            "timestamp": "2024-05-27T14:00:00Z",
            "user_id": f"user_{uuid.uuid4().hex[:8]}",
        }
        seq = f"ddb-check-{uuid.uuid4().hex}"
        event = self._make_kinesis_event(payload, seq)

        h.lambda_handler(event, None)

        # Verify the key was written to DynamoDB
        ddb = client("dynamodb")
        resp = ddb.get_item(
            TableName=DDB_TABLE,
            Key={"sequence_number": {"S": seq}},
        )
        assert "Item" in resp, f"Sequence number {seq} not found in DynamoDB"
        assert "processed_at" in resp["Item"]
        assert "ttl" in resp["Item"]


@localstack_only
class TestS3LandingZone:
    """Verify S3 bucket exists and is accessible."""

    def test_bucket_exists(self):
        s3 = client("s3")
        resp = s3.list_buckets()
        bucket_names = [b["Name"] for b in resp["Buckets"]]
        assert BUCKET in bucket_names, f"Bucket '{BUCKET}' not found in LocalStack"

    def test_can_write_and_read_object(self):
        s3 = client("s3")
        key = "events/year=2024/month=05/day=27/hour=14/test-object.json"
        body = json.dumps({"test": True, "ts": time.time()})

        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        content = json.loads(resp["Body"].read())

        assert content["test"] is True
