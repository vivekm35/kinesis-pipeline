##############################################################################
# main.tf — Kinesis Streaming Pipeline Infrastructure
#
# Resources:
#   • Kinesis Data Stream (10 shards, 24h retention)
#   • Lambda function (transformer) + IAM role
#   • Kinesis → Lambda event source mapping (bisect-on-error)
#   • SQS Dead-Letter Queue + Lambda ESM wiring
#   • DynamoDB table (idempotency keys, TTL enabled)
#   • Kinesis Firehose → S3
#   • S3 bucket (landing zone, versioning, lifecycle)
#   • Redshift Serverless namespace + workgroup
#   • CloudWatch Log Groups + alarms
##############################################################################

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "your-tfstate-bucket"
    key    = "kinesis-pipeline/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ── Data ────────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ── Kinesis Data Stream ──────────────────────────────────────────────────────

resource "aws_kinesis_stream" "events" {
  name             = "${local.name_prefix}-events"
  shard_count      = var.kinesis_shard_count
  retention_period = 24 # hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = local.common_tags
}

# ── SQS Dead-Letter Queue ────────────────────────────────────────────────────

resource "aws_sqs_queue" "dlq" {
  name                       = "${local.name_prefix}-events-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 300

  tags = local.common_tags
}

resource "aws_sqs_queue_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.dlq.arn
      Condition = {
        ArnLike = {
          "aws:SourceArn" = aws_lambda_function.transformer.arn
        }
      }
    }]
  })
}

# ── DynamoDB Idempotency Table ───────────────────────────────────────────────

resource "aws_dynamodb_table" "idempotency" {
  name         = "${local.name_prefix}-idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "sequence_number"

  attribute {
    name = "sequence_number"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery { enabled = true }

  tags = local.common_tags
}

# ── S3 Landing Zone ──────────────────────────────────────────────────────────

resource "aws_s3_bucket" "landing" {
  bucket = "${local.name_prefix}-landing-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    id     = "archive-old-events"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }

    expiration { days = 365 }
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM Role — Lambda ────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda" {
  name = "${local.name_prefix}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "lambda" {
  name = "pipeline-permissions"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords", "kinesis:GetShardIterator",
          "kinesis:DescribeStream", "kinesis:ListShards",
          "kinesis:ListStreams"
        ]
        Resource = aws_kinesis_stream.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
        Resource = aws_kinesis_firehose_delivery_stream.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem"]
        Resource = aws_dynamodb_table.idempotency.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.dlq.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ── Lambda Function ──────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name_prefix}-transformer"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_lambda_function" "transformer" {
  function_name = "${local.name_prefix}-transformer"
  role          = aws_iam_role.lambda.arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 300
  memory_size   = 512

  # Deploy from local zip; in CI this is replaced by S3 artifact
  filename         = "${path.module}/../../lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../lambda.zip")

  environment {
    variables = {
      FIREHOSE_DELIVERY_STREAM = aws_kinesis_firehose_delivery_stream.events.name
      IDEMPOTENCY_TABLE        = aws_dynamodb_table.idempotency.name
      DLQ_URL                  = aws_sqs_queue.dlq.url
      AWS_REGION               = var.aws_region
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
  tags       = local.common_tags
}

resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.transformer.arn
  starting_position                  = "LATEST"
  batch_size                         = 500
  maximum_batching_window_in_seconds = 5
  parallelization_factor             = 10 # 1 concurrent per shard
  bisect_batch_on_function_error     = true
  maximum_retry_attempts             = 3

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }
}

# ── IAM Role — Firehose ──────────────────────────────────────────────────────

resource "aws_iam_role" "firehose" {
  name = "${local.name_prefix}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "firehose" {
  name = "s3-write"
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:AbortMultipartUpload", "s3:GetBucketLocation",
        "s3:GetObject", "s3:ListBucket",
        "s3:ListBucketMultipartUploads", "s3:PutObject"
      ]
      Resource = [
        aws_s3_bucket.landing.arn,
        "${aws_s3_bucket.landing.arn}/*"
      ]
    }]
  })
}

# ── Kinesis Firehose → S3 ────────────────────────────────────────────────────

resource "aws_kinesis_firehose_delivery_stream" "events" {
  name        = "${local.name_prefix}-events-firehose"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = aws_s3_bucket.landing.arn
    buffering_size     = 128 # MB
    buffering_interval = 60  # seconds — triggers whichever comes first

    # Structured landing zone: year=YYYY/month=MM/day=DD/hour=HH/
    prefix              = "events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/!{firehose:error-output-type}/"

    compression_format = "GZIP"
  }

  tags = local.common_tags
}

# ── CloudWatch Alarms ────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.name_prefix}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "Lambda error rate exceeds 10/min"

  dimensions = {
    FunctionName = aws_lambda_function.transformer.function_name
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${local.name_prefix}-dlq-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  alarm_description   = "DLQ has > 100 messages — investigate failed records"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  tags = local.common_tags
}
