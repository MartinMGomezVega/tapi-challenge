variable "name_prefix" {
  type        = string
  description = "Resource naming prefix."
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region."
}

variable "api_gateway_execution_arn" {
  type        = string
  description = "Private internal API Gateway execution ARN (arn:aws:execute-api:region:account:api-id/stage/*)."
}

variable "execute_api_vpce_id" {
  type        = string
  default     = ""
  description = "Optional VPC Endpoint ID for execute-api used in API Gateway Resource Policy (006-T019)."
}

variable "github_repo" {
  type        = string
  default     = "martingomezvega/tapi-challenge"
  description = "GitHub repository (owner/repo) allowed to assume the OIDC deploy role."
}

variable "create_github_oidc_provider" {
  type        = bool
  default     = false
  description = "Create the account-level GitHub OIDC provider if not already present."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags."
}
