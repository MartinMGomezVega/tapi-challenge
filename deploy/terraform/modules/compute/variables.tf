variable "name_prefix" {
  type        = string
  description = "Resource naming prefix."
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region."
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Private subnet IDs across 3 AZs."
}

variable "ecs_security_group_id" {
  type        = string
  description = "Security group attached to ECS Fargate tasks."
}

variable "kms_key_arn" {
  type        = string
  description = "KMS CMK ARN for ECR repositories and CloudWatch Log Groups."
}

variable "ecs_execution_role_arn" {
  type = string
}

variable "dispatcher_task_role_arn" {
  type = string
}

variable "worker_task_role_arn" {
  type = string
}

variable "reconciler_task_role_arn" {
  type = string
}

variable "scheduler_role_arn" {
  type = string
}

variable "requests_table_name" {
  type = string
}

variable "executions_table_name" {
  type = string
}

variable "coordination_table_name" {
  type = string
}

variable "results_bucket_name" {
  type = string
}

variable "jobs_queue_url" {
  type = string
}

variable "jobs_queue_name" {
  type = string
}

variable "provider_api_base_url" {
  type        = string
  description = "Private API Gateway base URL reachable via execute-api VPC endpoint."
}

variable "image_tag" {
  type        = string
  default     = "latest"
  description = "Immutable git SHA image tag deployed to ECS."
}

variable "shards" {
  type    = number
  default = 4
}

variable "inflight_per_task" {
  type    = number
  default = 32
}

variable "lambda_budget" {
  type        = number
  default     = 1000
  description = "Account regional Lambda concurrent execution quota (006-T020)."
}

variable "worker_min_tasks" {
  type    = number
  default = 2
}

variable "run_day_tz" {
  type    = string
  default = "UTC"
}

variable "tags" {
  type    = map(string)
  default = {}
}
