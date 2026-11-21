output "ecs_cluster_arn" {
  value = aws_ecs_cluster.this.arn
}

output "worker_max_tasks" {
  value = local.worker_max_tasks
}

output "ecr_repository_urls" {
  value = { for k, v in aws_ecr_repository.service : k => v.repository_url }
}
