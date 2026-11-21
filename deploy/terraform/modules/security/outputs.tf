output "kms_key_arn" {
  value = aws_kms_key.this.arn
}

output "ecs_execution_role_arn" {
  value = aws_iam_role.ecs_execution.arn
}

output "dispatcher_task_role_arn" {
  value = aws_iam_role.dispatcher_task.arn
}

output "worker_task_role_arn" {
  value = aws_iam_role.worker_task.arn
}

output "reconciler_task_role_arn" {
  value = aws_iam_role.reconciler_task.arn
}

output "scheduler_role_arn" {
  value = aws_iam_role.scheduler.arn
}

output "github_oidc_role_arn" {
  value = aws_iam_role.github_oidc.arn
}

output "api_gateway_resource_policy_json" {
  value       = data.aws_iam_policy_document.api_gateway_resource_policy.json
  description = "Resource policy to attach to the private internal API Gateway (006-T019)."
}
