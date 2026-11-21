variable "env" {
  type        = string
  description = "Environment name (dev, staging, prod)."
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "azs" {
  type    = list(string)
  default = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "api_gateway_execution_arn" {
  type = string
}

variable "provider_api_base_url" {
  type = string
}

variable "github_repo" {
  type    = string
  default = "martingomezvega/tapi-challenge"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "shards" {
  type    = number
  default = 4
}

variable "inflight_per_task" {
  type    = number
  default = 32
}

variable "worker_min_tasks" {
  type    = number
  default = 2
}

variable "lambda_budget_override" {
  type        = number
  default     = 0
  description = "Optional override for LAMBDA_BUDGET; when 0, derived from AWS Service Quotas (L-B99A9384)."
}

variable "run_day_tz" {
  type    = string
  default = "UTC"
}

variable "alert_email" {
  type    = string
  default = ""
}
