# Operational Runbook

This runbook covers day-to-day operations, incident response, and common maintenance tasks for the Kinesis streaming pipeline.

---

## Table of contents

1. [Service inventory](#1-service-inventory)
2. [Health checks](#2-health-checks)
3. [Common incidents](#3-common-incidents)
4. [Scaling procedures](#4-scaling-procedures)
5. [DLQ management](#5-dlq-management)
6. [Redshift maintenance](#6-redshift-maintenance)
7. [Local development](#7-local-development)
8. [Deployment](#8-deployment)

---

## 1. Service inventory

| Service | Purpose | Key metric | Alarm threshold |
|---|---|---|---|
| Kinesis Data Stream | Event ingestion | IncomingRecords/s | > 900/s per shard |
| Lambda transformer | Decode, dedup, enrich | Errors/min | > 10 |
| SQS DLQ | Failed record capture | MessagesVisible | > 100 |
| DynamoDB | Idempotency keys | ConsumedWriteCapacity | throttled |
| Kinesis Firehose | Buffered S3 delivery | DataFreshness | > 120s |
| S3 landing zone | Raw event storage | — | — |
| Redshift | Analytics queries | WLM queue time | > 30s |

---

## 2. Health checks

### Quick status (run from terminal)

```bash
# Kinesis — stream status and shard count
aws kinesis describe-stream-summary \
  --stream-name $KINESIS_STREAM_NAME \
  --query 'StreamDescriptionSummary.[StreamStatus,OpenShardCount,RetentionPeriodHours]'

# Lambda — last 5 minutes of errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/kinesis-pipeline-prod-transformer \
  --start-time $(date -v -5M +%s000) \
  --filter-pattern "ERROR"

# SQS DLQ — message count
aws sqs get-queue-attributes \
  --queue-url $DLQ_URL \
  --attribute-names ApproximateNumberOfMessages

# Firehose — delivery health
aws firehose describe-delivery-stream \
  --delivery-stream-name $FIREHOSE_DELIVERY_STREAM \
  --query 'DeliveryStreamDescription.DeliveryStreamStatus'

# Redshift — active connections
psql -h $REDSHIFT_HOST -U $REDSHIFT_USER -d $REDSHIFT_DB \
  -c "SELECT COUNT(*) FROM stv_sessions WHERE user_name = '$REDSHIFT_USER';"
```

### Real-time shard monitor

```bash
python scripts/shard_monitor.py --stream $KINESIS_STREAM_NAME --interval 10
```

---

## 3. Common incidents

### INC-001: Lambda error rate spike

**Symptoms:** CloudWatch `lambda-errors` alarm fires. Error rate > 10/min.

**Triage:**
```bash
# See the actual error messages
aws logs filter-log-events \
  --log-group-name /aws/lambda/kinesis-pipeline-prod-transformer \
  --start-time $(date -v -15M +%s000) \
  --filter-pattern "ERROR" \
  --query 'events[].message'

# Check DLQ depth — are records piling up?
aws sqs get-queue-attributes \
  --queue-url $DLQ_URL \
  --attribute-names ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible
```

**Resolution by error type:**

| Error | Cause | Fix |
|---|---|---|
| `ValidationError: missing required fields` | Producer sending malformed events | Fix producer schema; DLQ records are safe to discard |
| `ClientError: ProvisionedThroughputExceededException` | Firehose or DynamoDB throttled | Increase capacity; Lambda will retry automatically |
| `ClientError: ResourceNotFoundException` | Firehose or DDB table deleted/renamed | Check Terraform state; redeploy if drift |
| `json.JSONDecodeError` | Corrupted records in stream | Records will be DLQ'd; investigate producer |

---

### INC-002: DLQ depth > 100

**Symptoms:** `dlq-depth` CloudWatch alarm fires.

**Triage:**
```bash
# Inspect a sample message (does not delete it)
python scripts/dlq_replay.py --dry-run --max-messages 5
```

**Resolution:**
1. Identify root cause from Lambda logs (see INC-001).
2. Fix the underlying bug.
3. Deploy the fix.
4. Replay DLQ records:

```bash
# Replay up to 500 records
python scripts/dlq_replay.py --max-messages 500

# For large backlogs, replay in batches
python scripts/dlq_replay.py --max-messages 1000
```

Lambda idempotency keys will deduplicate any records that were already processed before hitting the DLQ.

---

### INC-003: Kinesis shard throttling

**Symptoms:** `WriteProvisionedThroughputExceeded` CloudWatch metric > 0. Producer logs show throttle warnings.

**Cause:** A single shard is receiving > 1 MB/s or > 1,000 records/s.

**Triage:**
```bash
# Check which shards are hot
python scripts/shard_monitor.py --stream $KINESIS_STREAM_NAME
```

**Resolution:**
```bash
# Scale from 10 to 20 shards (no downtime, ~30 min to complete)
aws kinesis update-shard-count \
  --stream-name $KINESIS_STREAM_NAME \
  --target-shard-count 20 \
  --scaling-type UNIFORM_SCALING

# Update Terraform state to match
cd infrastructure/terraform
terraform apply -var kinesis_shard_count=20 -var-file envs/prod.tfvars
```

Also increase Lambda `parallelization_factor` to match new shard count.

---

### INC-004: Redshift query latency > 2s

**Triage:**
```bash
# Check for long-running queries
psql -h $REDSHIFT_HOST -U $REDSHIFT_USER -d $REDSHIFT_DB << 'SQL'
SELECT pid, query, elapsed, substring(querytxt, 1, 80) AS sql
FROM stv_recents
WHERE status = 'Running'
ORDER BY elapsed DESC;
SQL
```

**Resolution:**
```bash
# Run VACUUM + ANALYZE after large loads
psql -h $REDSHIFT_HOST -U $REDSHIFT_USER -d $REDSHIFT_DB \
  -f redshift/queries/maintenance.sql
```

---

### INC-005: S3 landing zone — Firehose DataFreshness > 120s

**Symptoms:** Firehose not delivering to S3 fast enough; reporting data is stale.

**Triage:**
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Firehose \
  --metric-name DeliveryToS3.DataFreshness \
  --dimensions Name=DeliveryStreamName,Value=$FIREHOSE_DELIVERY_STREAM \
  --start-time $(date -u -v -30M +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 \
  --statistics Maximum
```

**Resolution:** Usually transient S3 throttling. If persists > 10 min:
1. Check S3 bucket policy and IAM role.
2. Check S3 service health dashboard.
3. Check Firehose error prefix in S3 for failed delivery details.

---

## 4. Scaling procedures

### Scale Kinesis shards

```bash
# Check current shard count
aws kinesis describe-stream-summary \
  --stream-name $KINESIS_STREAM_NAME \
  --query StreamDescriptionSummary.OpenShardCount

# Scale up (doubling is the recommended increment)
aws kinesis update-shard-count \
  --stream-name $KINESIS_STREAM_NAME \
  --target-shard-count <NEW_COUNT> \
  --scaling-type UNIFORM_SCALING
```

### Update Lambda parallelization factor

Edit `infrastructure/terraform/main.tf`:
```hcl
resource "aws_lambda_event_source_mapping" "kinesis" {
  parallelization_factor = 20  # match new shard count
  ...
}
```

Then: `make infra-apply ENV=prod`

---

## 5. DLQ management

### View DLQ messages without deleting

```bash
python scripts/dlq_replay.py --dry-run --max-messages 20
```

### Replay DLQ to Kinesis

```bash
# Replay to same stream
python scripts/dlq_replay.py --max-messages 500

# Replay to staging for testing
python scripts/dlq_replay.py \
  --target-stream kinesis-pipeline-staging-events \
  --max-messages 100
```

### Purge DLQ (discard all messages — use with caution)

```bash
aws sqs purge-queue --queue-url $DLQ_URL
```

---

## 6. Redshift maintenance

### Run VACUUM + ANALYZE

```bash
psql -h $REDSHIFT_HOST -U $REDSHIFT_USER -d $REDSHIFT_DB \
  -f redshift/queries/maintenance.sql
```

### Manual COPY job (backfill a specific hour)

```bash
python redshift/copy_job.py \
  --s3-prefix "s3://$S3_BUCKET/events/year=2024/month=05/day=27/hour=14/"
```

### Verify no duplicate rows

```bash
psql -h $REDSHIFT_HOST -U $REDSHIFT_USER -d $REDSHIFT_DB \
  -c "SELECT event_id, COUNT(*) FROM analytics.events GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 10;"
```

---

## 7. Local development

### Start the full local stack

```bash
# Start LocalStack + local Postgres
docker compose -f docker/docker-compose.yml up -d

# Verify all services are healthy
docker compose -f docker/docker-compose.yml ps

# Tail logs
docker compose -f docker/docker-compose.yml logs -f localstack
```

### Run producer against LocalStack

```bash
source .venv/bin/activate
AWS_ENDPOINT_URL=http://localhost:4566 \
  python producer/producer.py \
    --stream kinesis-pipeline-dev-events \
    --rate 100 \
    --duration 60
```

### Run integration tests

```bash
# Requires LocalStack to be running
make test-integration
```

### Tear down local stack

```bash
docker compose -f docker/docker-compose.yml down -v
```

---

## 8. Deployment

### Deploy dev environment

```bash
make infra-plan ENV=dev   # review changes
make infra-apply ENV=dev  # apply
```

### Deploy Lambda code change

```bash
make package                           # build lambda.zip
aws lambda update-function-code \
  --function-name kinesis-pipeline-prod-transformer \
  --zip-file fileb://lambda.zip
```

### Roll back Lambda to previous version

```bash
# List recent versions
aws lambda list-versions-by-function \
  --function-name kinesis-pipeline-prod-transformer \
  --query 'Versions[*].[Version,LastModified]'

# Update alias to previous version
aws lambda update-alias \
  --function-name kinesis-pipeline-prod-transformer \
  --name live \
  --function-version <PREVIOUS_VERSION>
```

### Full prod deployment via CI

Push to `main` branch. GitHub Actions will:
1. Run lint + tests
2. Validate Terraform
3. Package Lambda
4. Apply Terraform (staging auto-deploy)

For prod: create a GitHub Release — triggers the `deploy-prod` workflow.
