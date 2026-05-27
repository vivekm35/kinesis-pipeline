"""
idempotency.py
--------------
DynamoDB-backed idempotency store.

Each processed Kinesis sequence number is written with a conditional
PutItem. If the item already exists the condition fails silently —
the record is a duplicate and is skipped.

TTL is set to 24 hours so the table self-cleans without manual pruning.
"""

import logging
import time
from typing import Optional

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

# How long to keep processed keys before auto-expiry (seconds)
KEY_TTL_SECS = 86_400  # 24 hours


class IdempotencyStore:
    def __init__(self, table_name: str, region: str = "us-east-1") -> None:
        self._table_name = table_name
        self._client = boto3.client("dynamodb", region_name=region)

    def is_duplicate(self, sequence_number: str) -> bool:
        """Return True if this sequence number has already been processed."""
        try:
            resp = self._client.get_item(
                TableName=self._table_name,
                Key={"sequence_number": {"S": sequence_number}},
                ProjectionExpression="sequence_number",
            )
            return "Item" in resp
        except ClientError as exc:
            # On DynamoDB errors, assume not duplicate (fail open) so we
            # don't silently drop events. The duplicate-write check below
            # provides a second safety net.
            log.warning("DynamoDB get_item error: %s — treating as non-duplicate", exc)
            return False

    def mark_processed(self, sequence_number: str) -> None:
        """
        Write the sequence number with a TTL.
        Uses a condition so concurrent Lambdas can't double-write.
        """
        expire_at = int(time.time()) + KEY_TTL_SECS
        try:
            self._client.put_item(
                TableName=self._table_name,
                Item={
                    "sequence_number": {"S": sequence_number},
                    "processed_at": {"N": str(int(time.time()))},
                    "ttl": {"N": str(expire_at)},
                },
                ConditionExpression="attribute_not_exists(sequence_number)",
            )
        except self._client.exceptions.ConditionalCheckFailedException:
            # Another Lambda instance already wrote this — safe to ignore
            log.debug("Race-condition duplicate for seq=%s — already marked", sequence_number)
        except ClientError as exc:
            log.error("DynamoDB put_item error for seq=%s: %s", sequence_number, exc)
            raise
