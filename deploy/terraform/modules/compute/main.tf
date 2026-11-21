locals {
  services = toset(["dispatcher", "worker", "reconciler"])
  # Enforce k * t <= LAMBDA_BUDGET / 2 directly in Terraform autoscaling cap (003 US4, 006-T020).
  worker_max_tasks = max(var.worker_min_tasks, floor(var.lambda_budget / (2 * var.inflight_per_task)))
}

# Immutable KMS-encrypted ECR repositories with scan_on_push enabled.
resource "aws_ecr_repository" "service" {
  for_each             = local.services
  name                 = "${var.name_prefix}-${each.key}"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = var.kms_key_arn
  }

  tags = var.tags
}

resource "aws_ecr_lifecycle_policy" "service" {
  for_each   = aws_ecr_repository.service
  repository = each.value.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 20 immutable images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = { type = "expire" }
      }
    ]
  })
}

# KMS-encrypted CloudWatch Log Groups (365-day retention per CKV_AWS_338).
resource "aws_cloudwatch_log_group" "service" {
  for_each          = local.services
  name              = "/ecs/${var.name_prefix}-${each.key}"
  retention_in_days = 365
  kms_key_id        = var.kms_key_arn
  tags              = var.tags
}

# ECS Fargate Cluster with Container Insights enabled.
resource "aws_ecs_cluster" "this" {
  name = "${var.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = var.tags
}

# 1. Dispatcher Task Definition (ARM64 Graviton, readonly root FS, 2 replicas fixed).
resource "aws_ecs_task_definition" "dispatcher" {
  family                   = "${var.name_prefix}-dispatcher"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.dispatcher_task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name                   = "dispatcher"
      image                  = "${aws_ecr_repository.service["dispatcher"].repository_url}:${var.image_tag}"
      essential              = true
      readonlyRootFilesystem = true
      portMappings           = [{ containerPort = 8080, protocol = "tcp" }]
      environment = [
        { name = "AWS_REGION", value = var.region },
        { name = "AWS_DEFAULT_REGION", value = var.region },
        { name = "REQUESTS_TABLE", value = var.requests_table_name },
        { name = "EXECUTIONS_TABLE", value = var.executions_table_name },
        { name = "COORDINATION_TABLE", value = var.coordination_table_name },
        { name = "JOBS_QUEUE_URL", value = var.jobs_queue_url },
        { name = "SHARDS", value = tostring(var.shards) },
        { name = "RUN_DAY_TZ", value = var.run_day_tz },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.service["dispatcher"].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "dispatcher"
        }
      }
    }
  ])

  tags = var.tags
}

resource "aws_ecs_service" "dispatcher" {
  name            = "${var.name_prefix}-dispatcher"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.dispatcher.arn
  desired_count   = 2
  launch_type     = "FARGATE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.ecs_security_group_id]
    assign_public_ip = false
  }

  tags = var.tags
}

# 2. Worker Task Definition (ARM64 Graviton, readonly root FS, LAMBDA_BUDGET derived from Service Quotas).
resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.name_prefix}-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.worker_task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name                   = "worker"
      image                  = "${aws_ecr_repository.service["worker"].repository_url}:${var.image_tag}"
      essential              = true
      readonlyRootFilesystem = true
      stopTimeout            = 30
      portMappings           = [{ containerPort = 8081, protocol = "tcp" }]
      environment = [
        { name = "AWS_REGION", value = var.region },
        { name = "REQUESTS_TABLE", value = var.requests_table_name },
        { name = "EXECUTIONS_TABLE", value = var.executions_table_name },
        { name = "RESULTS_BUCKET", value = var.results_bucket_name },
        { name = "JOBS_QUEUE_URL", value = var.jobs_queue_url },
        { name = "PROVIDER_API_BASE_URL", value = var.provider_api_base_url },
        { name = "INFLIGHT_PER_TASK", value = tostring(var.inflight_per_task) },
        { name = "MAX_TASKS", value = tostring(local.worker_max_tasks) },
        { name = "LAMBDA_BUDGET", value = tostring(var.lambda_budget) },
        { name = "STOP_TIMEOUT_SEC", value = "30" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.service["worker"].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "worker"
        }
      }
    }
  ])

  tags = var.tags
}

resource "aws_ecs_service" "worker" {
  name            = "${var.name_prefix}-worker"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_min_tasks
  launch_type     = "FARGATE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.ecs_security_group_id]
    assign_public_ip = false
  }

  lifecycle {
    ignore_changes = [desired_count]
  }

  tags = var.tags
}

# Worker Autoscaling bounded by LAMBDA_BUDGET (003 FR-008, 006 FR-003, T020).
resource "aws_appautoscaling_target" "worker" {
  max_capacity       = local.worker_max_tasks
  min_capacity       = var.worker_min_tasks
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "worker_queue_age" {
  name               = "${var.name_prefix}-worker-queue-age-tracking"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  service_namespace  = aws_appautoscaling_target.worker.service_namespace

  target_tracking_scaling_policy_configuration {
    target_value       = 300 # target oldest message age <= 5 min
    scale_in_cooldown  = 300
    scale_out_cooldown = 60

    customized_metric_specification {
      metric_name = "ApproximateAgeOfOldestMessage"
      namespace   = "AWS/SQS"
      statistic   = "Maximum"
      dimensions {
        name  = "QueueName"
        value = var.jobs_queue_name
      }
    }
  }
}

# 3. Daily Reconciler Task Definition + EventBridge Scheduler at 00:30 (005 FR-001).
resource "aws_ecs_task_definition" "reconciler" {
  family                   = "${var.name_prefix}-reconciler"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = var.ecs_execution_role_arn
  task_role_arn            = var.reconciler_task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name                   = "reconciler"
      image                  = "${aws_ecr_repository.service["reconciler"].repository_url}:${var.image_tag}"
      essential              = true
      readonlyRootFilesystem = true
      environment = [
        { name = "AWS_REGION", value = var.region },
        { name = "REQUESTS_TABLE", value = var.requests_table_name },
        { name = "EXECUTIONS_TABLE", value = var.executions_table_name },
        { name = "COORDINATION_TABLE", value = var.coordination_table_name },
        { name = "RESULTS_BUCKET", value = var.results_bucket_name },
        { name = "JOBS_QUEUE_URL", value = var.jobs_queue_url },
        { name = "SHARDS", value = tostring(var.shards) },
        { name = "RUN_DAY_TZ", value = var.run_day_tz },
        { name = "RECONCILE_REDISPATCH", value = "true" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.service["reconciler"].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "reconciler"
        }
      }
    }
  ])

  tags = var.tags
}

resource "aws_scheduler_schedule" "daily_reconciler" {
  name                         = "${var.name_prefix}-daily-reconciler"
  schedule_expression          = "cron(30 0 * * ? *)"
  schedule_expression_timezone = var.run_day_tz
  kms_key_arn                  = var.kms_key_arn

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.this.arn
    role_arn = var.scheduler_role_arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.reconciler.arn
      launch_type         = "FARGATE"
      task_count          = 1

      network_configuration {
        subnets          = var.private_subnet_ids
        security_groups  = [var.ecs_security_group_id]
        assign_public_ip = false
      }
    }
  }
}
