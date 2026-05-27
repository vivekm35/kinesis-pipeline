"""
tests/test_handler.py
---------------------
Unit tests for the Lambda transformer using moto to mock AWS services.
"""

import base64
import json
import os
import unittest
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

# ── Set required env vars before importing handler ───────────────────────────
os.environ.setdefault("FIREHOSE_DELIVERY_STREAM", "test-firehose")
os.environ.setdefault("IDEMPOTENCY_TABLE", "test-idempotency")
os.environ.setdefault("DLQ_URL", "https://sqs.us-east-1.amazonaws.com/123/test-dlq")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


def _make_kinesis_record(payload: dict, sequence: str = "seq-001") -> dict:
    """Build a minimal Kinesis ESM record."""
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    return {
        "kinesis": {
            "sequenceNumber": sequence,
            "data": encoded,
            "partitionKey": "user_001",
        },
        "eventSource": "aws:kinesis",
    }


def _valid_payload(event_id: str = "evt-001") -> dict:
    return {
        "event_id": event_id,
        "event_type": "page_view",
        "timestamp": "2024-05-27T14:00:00Z",
        "user_id": "user_001",
        "session_id": "sess-001",
    }


# ── Tests ────────────────────────────────────────────────────────────────────


class TestDecodeKinesisRecord(unittest.TestCase):
    def test_decodes_valid_record(self):
        from lambda_fn.handler import decode_kinesis_record

        payload = _valid_payload()
        record = _make_kinesis_record(payload)
        result = decode_kinesis_record(record)
        assert result["event_id"] == "evt-001"

    def test_raises_on_invalid_json(self):
        from lambda_fn.handler import decode_kinesis_record

        bad_data = base64.b64encode(b"not-json").decode()
        record = {"kinesis": {"data": bad_data, "sequenceNumber": "seq-x"}}
        with pytest.raises(json.JSONDecodeError):
            decode_kinesis_record(record)


class TestTransform(unittest.TestCase):
    def test_enriches_valid_payload(self):
        from lambda_fn.handler import transform

        payload = _valid_payload()
        result = transform(payload, "seq-001")
        assert "_meta" in result
        assert result["_meta"]["kinesis_sequence"] == "seq-001"

    def test_raises_on_missing_fields(self):
        from lambda_fn.handler import transform

        payload = {"event_id": "x"}  # missing event_type, timestamp, user_id
        with pytest.raises(ValueError, match="missing required fields"):
            transform(payload, "seq-001")

    def test_preserves_original_fields(self):
        from lambda_fn.handler import transform

        payload = _valid_payload()
        result = transform(payload, "seq-001")
        assert result["event_type"] == "page_view"
        assert result["user_id"] == "user_001"


@mock_aws
class TestLambdaHandler(unittest.TestCase):
    def setUp(self):
        # Create mock DynamoDB table
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="test-idempotency",
            KeySchema=[{"AttributeName": "sequence_number", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "sequence_number", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        # Create mock S3 bucket (required before Firehose)
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        # Create mock Firehose delivery stream
        fh = boto3.client("firehose", region_name="us-east-1")
        fh.create_delivery_stream(
            DeliveryStreamName="test-firehose",
            S3DestinationConfiguration={
                "RoleARN": "arn:aws:iam::123456789012:role/test",
                "BucketARN": "arn:aws:s3:::test-bucket",
            },
        )

    def _fresh_handler(self):
        """Re-import handler so it picks up the fresh mocked clients."""
        import importlib
        import lambda_fn.handler as h

        importlib.reload(h)
        return h

    def test_processes_valid_batch(self):
        h = self._fresh_handler()
        event = {"Records": [_make_kinesis_record(_valid_payload(), "seq-001")]}
        result = h.lambda_handler(event, MagicMock())
        assert result["processed"] == 1
        assert result["skipped"] == 0
        assert result["errors"] == 0

    def test_skips_duplicate_sequence(self):
        h = self._fresh_handler()
        record = _make_kinesis_record(_valid_payload(), "seq-dup")
        event = {"Records": [record]}
        # First call — processes
        h.lambda_handler(event, MagicMock())
        # Second call with same sequence — should skip
        result = h.lambda_handler(event, MagicMock())
        assert result["skipped"] == 1

    def test_skips_malformed_record_without_raising(self):
        h = self._fresh_handler()
        bad_data = base64.b64encode(b"not-json").decode()
        bad_record = {
            "kinesis": {"sequenceNumber": "seq-bad", "data": bad_data, "partitionKey": "k"},
            "eventSource": "aws:kinesis",
        }
        event = {"Records": [bad_record]}
        result = h.lambda_handler(event, MagicMock())
        assert result["errors"] == 1
        assert result["processed"] == 0

    def test_empty_batch_returns_ok(self):
        h = self._fresh_handler()
        result = h.lambda_handler({"Records": []}, MagicMock())
        assert result["statusCode"] == 200
        assert result["processed"] == 0


class TestIdempotencyStore(unittest.TestCase):
    @mock_aws
    def test_is_duplicate_returns_false_for_new_key(self):
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="test-idempotency",
            KeySchema=[{"AttributeName": "sequence_number", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "sequence_number", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        from lambda_fn.idempotency import IdempotencyStore

        store = IdempotencyStore("test-idempotency", "us-east-1")
        assert store.is_duplicate("new-seq-123") is False

    @mock_aws
    def test_is_duplicate_returns_true_after_mark(self):
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName="test-idempotency",
            KeySchema=[{"AttributeName": "sequence_number", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "sequence_number", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        from lambda_fn.idempotency import IdempotencyStore

        store = IdempotencyStore("test-idempotency", "us-east-1")
        store.mark_processed("seq-abc")
        assert store.is_duplicate("seq-abc") is True
