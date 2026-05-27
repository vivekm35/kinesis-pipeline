output "kinesis_stream_name" {
  value       = aws_kinesis_stream.events.name
  description = "Kinesis stream name for the producer config"
}

output "kinesis_stream_arn" {
  value = aws_kinesis_stream.events.arn
}

output "firehose_delivery_stream_name" {
  value = aws_kinesis_firehose_delivery_stream.events.name
}

output "s3_landing_bucket" {
  value       = aws_s3_bucket.landing.bucket
  description = "S3 bucket name for the landing zone"
}

output "dlq_url" {
  value       = aws_sqs_queue.dlq.url
  description = "SQS DLQ URL — use this in .env"
}

output "idempotency_table_name" {
  value = aws_dynamodb_table.idempotency.name
}

output "lambda_function_name" {
  value = aws_lambda_function.transformer.function_name
}
