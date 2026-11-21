variable "name_prefix" {
  type        = string
  description = "Resource naming prefix."
}

variable "kms_key_arn" {
  type        = string
  description = "KMS CMK ARN for SQS encryption."
}

variable "visibility_timeout_seconds" {
  type        = number
  default     = 60
  description = "Default visibility timeout for main FIFO queue."
}

variable "max_receive_count" {
  type        = number
  default     = 6
  description = "Number of receives before routing to FIFO DLQ (004 FR-004)."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags."
}
