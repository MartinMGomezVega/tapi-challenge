variable "name_prefix" {
  type        = string
  description = "Resource naming prefix (e.g. tapi-dev)."
}

variable "vpc_cidr" {
  type        = string
  default     = "10.20.0.0/16"
  description = "VPC CIDR block."
}

variable "azs" {
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
  description = "Three availability zones for private subnets."
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region for VPC endpoint service names."
}

variable "kms_key_arn" {
  type        = string
  description = "KMS CMK ARN for VPC Flow Logs log group encryption."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags."
}
