#!/usr/bin/env bash
# scripts/seed_localstack.sh
# ---------------------------
# Seeds LocalStack with every AWS resource the pipeline needs.
# Run automatically by the pipeline-init Docker service.
# Can also be run manually:
#   AWS_ENDPOINT_URL=http://localhost:4566 bash scripts/seed_localstack.sh

set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="us-east-1"
ACCOUNT="000000000000"   # LocalStack fake account ID

AWS="aws --endpoint-url=$ENDPOINT --region=$REGION"

echo "▶  Seeding LocalStack at $ENDPOINT ..."

# ── S3 landing zone ──────────────────────────────────────────────────────────
echo "   S3: creating landing zone bucket..."
$AWS s3api create-bucket \
  --bucket kinesis-pipeline-dev-landing \
  --region $REGION 2>/dev/null || echo "   S3: bucket already exists"

# ── DynamoDB idempotency table ───────────────────────────────────────────────
echo "   DynamoDB: creating idempotency table..."
$AWS dynamodb create-table \
  --table-name kinesis-pipeline-dev-idempotency \
  --attribute-definitions AttributeName=sequence_number,AttributeType=S \
  --key-schema AttributeName=sequence_number,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --no-cli-pager 2>/dev/null || echo "   DynamoDB: table already exists"

$AWS dynamodb update-time-to-live \
  --table-name kinesis-pipeline-dev-idempotency \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" \
  --no-cli-pager 2>/dev/null || true

# ── SQS dead-letter queue ────────────────────────────────────────────────────
echo "   SQS: creating dead-letter queue..."
DLQ_URL=$($AWS sqs create-queue \
  --queue-name kinesis-pipeline-dev-events-dlq \
  --attributes MessageRetentionPeriod=1209600 \
  --query QueueUrl --output text 2>/dev/null || \
  $AWS sqs get-queue-url --queue-name kinesis-pipeline-dev-events-dlq --query QueueUrl --output text)
echo "   SQS DLQ URL: $DLQ_URL"

# ── Kinesis data stream ──────────────────────────────────────────────────────
echo "   Kinesis: creating stream with 2 shards (dev)..."
$AWS kinesis create-stream \
  --stream-name kinesis-pipeline-dev-events \
  --shard-count 2 \
  --no-cli-pager 2>/dev/null || echo "   Kinesis: stream already exists"

# Wait for stream to become ACTIVE
echo "   Kinesis: waiting for stream to be ACTIVE..."
for i in {1..20}; do
  STATUS=$($AWS kinesis describe-stream-summary \
    --stream-name kinesis-pipeline-dev-events \
    --query 'StreamDescriptionSummary.StreamStatus' \
    --output text 2>/dev/null || echo "CREATING")
  if [ "$STATUS" = "ACTIVE" ]; then
    echo "   Kinesis: stream is ACTIVE"
    break
  fi
  echo "   Kinesis: status=$STATUS, waiting 2s..."
  sleep 2
done

# ── Kinesis Firehose → S3 ────────────────────────────────────────────────────
echo "   Firehose: creating delivery stream..."
$AWS firehose create-delivery-stream \
  --delivery-stream-name kinesis-pipeline-dev-events-firehose \
  --s3-destination-configuration \
    "RoleARN=arn:aws:iam::${ACCOUNT}:role/firehose-role,\
BucketARN=arn:aws:s3:::kinesis-pipeline-dev-landing,\
Prefix=events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/,\
BufferingHints={SizeInMBs=1,IntervalInSeconds=60}" \
  --no-cli-pager 2>/dev/null || echo "   Firehose: stream already exists"

echo ""
echo "✅  LocalStack seeded successfully!"
echo ""
echo "   Stream:    kinesis-pipeline-dev-events"
echo "   Firehose:  kinesis-pipeline-dev-events-firehose"
echo "   S3 bucket: kinesis-pipeline-dev-landing"
echo "   DynamoDB:  kinesis-pipeline-dev-idempotency"
echo "   SQS DLQ:   $DLQ_URL"
echo ""
echo "   Run the producer against LocalStack:"
echo "   AWS_ENDPOINT_URL=http://localhost:4566 python producer/producer.py \\"
echo "     --stream kinesis-pipeline-dev-events --rate 100 --duration 30"
