variable "name_prefix" {
  type        = string
  description = "Resource naming prefix."
}

variable "kms_key_arn" {
  type        = string
  description = "KMS CMK ARN used for DynamoDB tables and S3 results bucket encryption."
}

variable "results_retention_days" {
  type        = number
  default     = 90
  description = "Number of days to retain oversized results in S3 before expiration."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags."
}
