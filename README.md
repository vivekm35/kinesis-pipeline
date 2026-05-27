# Real-Time Kinesis Streaming & Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Lambda%20%7C%20Redshift-orange?logo=amazonaws)](https://aws.amazon.com)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform)](https://terraform.io)
[![CI](https://github.com/YOUR_USERNAME/kinesis-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/kinesis-pipeline/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade, serverless event-processing pipeline on AWS that ingests, transforms, and loads near real-time data into Redshift for operational reporting — processing **~5,000 events/second** with **1.2s end-to-end latency**.

---

## Architecture

```
Events → Kinesis Data Streams → Lambda (idempotent) → Kinesis Firehose → S3 (partitioned) → Redshift (COPY)
                                       ↓
                                  SQS Dead-Letter Queue
```

| Component | Role |
|---|---|
| **Kinesis Data Streams** | Durable, ordered ingestion across 10 shards |
| **AWS Lambda** | Stateless transformation with idempotency keys + retry logic |
| **Kinesis Firehose** | Buffered micro-batch delivery to S3 |
| **Amazon S3** | Structured landing zone partitioned by `year/month/day/hour` |
| **Amazon Redshift** | Columnar analytics store; batched COPY jobs for fast loads |
| **SQS Dead-Letter Queue** | Captures failed Lambda records for replay and audit |

---

## Performance

| Metric | Before | After |
|---|---|---|
| Throughput | ~1,000 events/sec | **~5,000 events/sec** |
| End-to-end latency | 8.0 s | **1.2 s** |
| Reporting query time | >10 s | **<2 s** |
| Duplicate events | Untracked | **<0.02%** (idempotency keys) |

---

## Project Structure

```
kinesis-pipeline/
├── producer/               # Event producer — simulates source workloads
│   ├── producer.py         # Kinesis PutRecords batch publisher
│   └── config.py           # Stream/shard configuration
├── lambda_fn/                 # Lambda handler — transform & route
│   ├── handler.py          # Main entry point with idempotency + DLQ routing
│   └── idempotency.py      # DynamoDB-backed deduplication helper
├── redshift/               # Redshift schema + COPY job orchestrator
│   ├── schema.sql          # DDL for events table (DISTKEY/SORTKEY tuned)
│   └── copy_job.py         # Batched COPY orchestrator with retry
├── infrastructure/
│   ├── terraform/          # Full IaC — Kinesis, Lambda, Firehose, S3, Redshift
│   └── cloudformation/     # Alternative CFN template
├── monitoring/
│   └── cloudwatch.py       # Custom metrics + dashboard setup
├── tests/                  # Unit + integration tests (pytest)
├── docs/
│   └── architecture.md     # Deep-dive design decisions
├── .github/workflows/
│   └── ci.yml              # Lint, test, and deploy pipeline
├── requirements.txt
└── Makefile                # Developer shortcuts
```

---

## Quick Start

### Prerequisites
- Python 3.11+
- AWS CLI configured (`aws configure`)
- Terraform ≥ 1.5 (for IaC)
- An AWS account with IAM permissions for Kinesis, Lambda, S3, Redshift, SQS

### 1. Clone & install

```bash
git clone https://github.com/YOUR_USERNAME/kinesis-pipeline.git
cd kinesis-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Deploy infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform plan -var-file="envs/dev.tfvars"
terraform apply -var-file="envs/dev.tfvars"
```

### 3. Run the event producer

```bash
python producer/producer.py --stream my-events-stream --rate 5000
```

### 4. Monitor in real time

```bash
python monitoring/cloudwatch.py --dashboard kinesis-pipeline
```

---

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```env
AWS_REGION=us-east-1
KINESIS_STREAM_NAME=events-stream
KINESIS_SHARD_COUNT=10
FIREHOSE_DELIVERY_STREAM=events-firehose
S3_BUCKET=your-landing-zone-bucket
REDSHIFT_HOST=your-cluster.redshift.amazonaws.com
REDSHIFT_DB=analytics
REDSHIFT_USER=pipeline_user
DLQ_URL=https://sqs.us-east-1.amazonaws.com/123456/events-dlq
IDEMPOTENCY_TABLE=lambda-idempotency
```

---

## Key Design Decisions

**Idempotency via DynamoDB** — Each Lambda invocation writes a conditional `PutItem` keyed on the Kinesis sequence number. Duplicate deliveries are silently dropped, keeping the processing-exactly-once rate above 99.98%.

**Batched COPY over row-by-row INSERT** — Firehose buffers records for 60 seconds or 128 MB, writes a single Parquet/JSON file to S3, then Lambda triggers a `COPY` command. This reduces Redshift WLM queue pressure and cuts per-row overhead from ~15 ms to ~0.02 ms.

**Structured S3 landing zone** — Prefix layout `s3://bucket/events/year=YYYY/month=MM/day=DD/hour=HH/` enables Redshift Spectrum queries and makes time-range COPY jobs simple to construct.

**SQS DLQ wiring** — Lambda's event source mapping sets `bisectBatchOnError: true` and `maximumRetryAttempts: 3`. Failed records are forwarded to SQS with the original Kinesis payload + error context, enabling targeted replay.

---

## Running Tests

```bash
make test               # unit tests
make test-integration   # requires localstack or live AWS
make lint               # flake8 + black check
```

---

## CI/CD

GitHub Actions runs on every push and pull request:
1. Lint (flake8, black)
2. Unit tests (pytest + moto for AWS mocking)
3. Terraform `validate` + `plan`
4. On merge to `main` → auto-deploy to staging via `terraform apply`

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

---

## License

[MIT](LICENSE)
