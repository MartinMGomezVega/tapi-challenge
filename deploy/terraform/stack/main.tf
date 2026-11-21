locals {
  name_prefix = "tapi-${var.env}"
  common_tags = {
    Project     = "tapi-scheduled-queries"
    Environment = var.env
    ManagedBy   = "terraform"
  }
  lambda_budget = var.lambda_budget_override > 0 ? var.lambda_budget_override : floor(data.aws_servicequotas_service_quota.lambda_concurrency.value)
}

# Derive account regional Lambda concurrency quota directly from AWS Service Quotas (006-T020, C3).
data "aws_servicequotas_service_quota" "lambda_concurrency" {
  service_code = "lambda"
  quota_code   = "L-B99A9384" # Concurrent executions
}

module "security" {
  source                    = "../modules/security"
  name_prefix               = local.name_prefix
  region                    = var.region
  api_gateway_execution_arn = var.api_gateway_execution_arn
  github_repo               = var.github_repo
  tags                      = local.common_tags
}

module "network" {
  source      = "../modules/network"
  name_prefix = local.name_prefix
  vpc_cidr    = var.vpc_cidr
  azs         = var.azs
  region      = var.region
  kms_key_arn = module.security.kms_key_arn
  tags        = local.common_tags
}

module "data" {
  source      = "../modules/data"
  name_prefix = local.name_prefix
  kms_key_arn = module.security.kms_key_arn
  tags        = local.common_tags
}

module "messaging" {
  source      = "../modules/messaging"
  name_prefix = local.name_prefix
  kms_key_arn = module.security.kms_key_arn
  tags        = local.common_tags
}

module "compute" {
  source                   = "../modules/compute"
  name_prefix              = local.name_prefix
  region                   = var.region
  private_subnet_ids       = module.network.private_subnet_ids
  ecs_security_group_id    = module.network.ecs_tasks_security_group_id
  kms_key_arn              = module.security.kms_key_arn
  ecs_execution_role_arn   = module.security.ecs_execution_role_arn
  dispatcher_task_role_arn = module.security.dispatcher_task_role_arn
  worker_task_role_arn     = module.security.worker_task_role_arn
  reconciler_task_role_arn = module.security.reconciler_task_role_arn
  scheduler_role_arn       = module.security.scheduler_role_arn
  requests_table_name      = module.data.requests_table_name
  executions_table_name    = module.data.executions_table_name
  coordination_table_name  = module.data.coordination_table_name
  results_bucket_name      = module.data.results_bucket_name
  jobs_queue_url           = module.messaging.jobs_queue_url
  jobs_queue_name          = module.messaging.jobs_queue_name
  provider_api_base_url    = var.provider_api_base_url
  image_tag                = var.image_tag
  shards                   = var.shards
  inflight_per_task        = var.inflight_per_task
  lambda_budget            = local.lambda_budget
  worker_min_tasks         = var.worker_min_tasks
  run_day_tz               = var.run_day_tz
  tags                     = local.common_tags
}

module "observability" {
  source              = "../modules/observability"
  name_prefix         = local.name_prefix
  jobs_queue_name     = module.messaging.jobs_queue_name
  dlq_queue_name      = module.messaging.dlq_queue_name
  results_bucket_name = module.data.results_bucket_name
  lambda_budget       = local.lambda_budget
  kms_key_arn         = module.security.kms_key_arn
  alert_email         = var.alert_email
  tags                = local.common_tags
}
