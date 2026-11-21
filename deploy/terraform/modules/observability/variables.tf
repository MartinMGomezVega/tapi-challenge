variable "name_prefix" {
  type        = string
  description = "Resource name prefix (e.g. tapi-prod)."
}

variable "metric_namespace" {
  type        = string
  default     = "Tapi/ScheduledQueries"
  description = "CloudWatch EMF namespace."
}

variable "jobs_queue_name" {
  type        = string
  description = "Main SQS FIFO queue name."
}

variable "dlq_queue_name" {
  type        = string
  description = "DLQ SQS FIFO queue name."
}

variable "results_bucket_name" {
  type        = string
  description = "S3 bucket name storing reconciliation reports."
}

variable "lambda_budget" {
  type        = number
  default     = 1000
  description = "Regional AWS Lambda concurrent execution quota."
}

variable "kms_key_arn" {
  type        = string
  description = "KMS CMK ARN used to encrypt SNS topic and CloudWatch resources."
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional email subscription for SNS operational alerts."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common resource tags."
}
