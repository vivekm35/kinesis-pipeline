"""
tests/conftest.py — shared pytest configuration.
"""

import os
import sys

# Make package roots importable from tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub AWS credentials for moto — must be set before any boto3 import
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_REGION", "us-east-1")

# Lambda env vars (mocked values)
os.environ.setdefault("FIREHOSE_DELIVERY_STREAM", "test-firehose")
os.environ.setdefault("IDEMPOTENCY_TABLE", "test-idempotency")
os.environ.setdefault("DLQ_URL", "https://sqs.us-east-1.amazonaws.com/000000000000/test-dlq")
os.environ.setdefault("KINESIS_STREAM_NAME", "test-stream")
