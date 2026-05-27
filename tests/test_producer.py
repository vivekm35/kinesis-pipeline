"""
tests/test_producer.py
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("KINESIS_STREAM_NAME", "test-stream")


class TestBuildEvent(unittest.TestCase):
    def test_has_required_fields(self):
        from producer.producer import build_event

        evt = build_event()
        for field in ("event_id", "event_type", "timestamp", "user_id", "session_id"):
            assert field in evt, f"Missing field: {field}"

    def test_event_type_override(self):
        from producer.producer import build_event

        evt = build_event(event_type="purchase")
        assert evt["event_type"] == "purchase"

    def test_event_is_json_serializable(self):
        from producer.producer import build_event

        evt = build_event()
        payload = json.dumps(evt)
        assert isinstance(payload, str)


class TestPutRecordsBatch(unittest.TestCase):
    def test_returns_counts_on_success(self):
        from producer.producer import build_event, put_records_batch

        mock_client = MagicMock()
        mock_client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [{"SequenceNumber": "s", "ShardId": "x"}],
        }
        records = [build_event() for _ in range(5)]
        result = put_records_batch(mock_client, "test-stream", records)
        assert result["sent"] == 5
        assert result["failed"] == 0

    def test_handles_partial_failure(self):
        from producer.producer import build_event, put_records_batch

        mock_client = MagicMock()
        mock_client.put_records.return_value = {
            "FailedRecordCount": 2,
            "Records": [],
        }
        records = [build_event() for _ in range(5)]
        result = put_records_batch(mock_client, "test-stream", records)
        assert result["failed"] == 2
        assert result["sent"] == 3

    def test_handles_client_error(self):
        from botocore.exceptions import ClientError

        from producer.producer import build_event, put_records_batch

        mock_client = MagicMock()
        mock_client.put_records.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttle"}},
            "PutRecords",
        )
        records = [build_event() for _ in range(3)]
        result = put_records_batch(mock_client, "test-stream", records)
        assert result["sent"] == 0
        assert result["failed"] == 3
