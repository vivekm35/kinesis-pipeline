"""
config.py — producer configuration loaded from environment variables.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProducerConfig:
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    KINESIS_STREAM_NAME: str = os.getenv("KINESIS_STREAM_NAME", "events-stream")
    KINESIS_SHARD_COUNT: int = int(os.getenv("KINESIS_SHARD_COUNT", "10"))
