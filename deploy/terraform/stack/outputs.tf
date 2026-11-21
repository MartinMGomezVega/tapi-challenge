output "ecs_cluster_arn" {
  value = module.compute.ecs_cluster_arn
}

output "jobs_queue_url" {
  value = module.messaging.jobs_queue_url
}

output "dlq_queue_url" {
  value = module.messaging.dlq_queue_url
}

output "execute_api_vpce_id" {
  value = module.network.execute_api_vpce_id
}

output "worker_max_tasks" {
  value = module.compute.worker_max_tasks
}

output "lambda_budget" {
  value = local.lambda_budget
}

output "dashboard_name" {
  value = module.observability.dashboard_name
}
