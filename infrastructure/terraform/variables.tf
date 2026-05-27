variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "kinesis-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards (each handles ~1 MB/s write)"
  type        = number
  default     = 10
}
