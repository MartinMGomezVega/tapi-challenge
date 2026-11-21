data "aws_caller_identity" "current" {}

locals {
  account_id       = data.aws_caller_identity.current.account_id
  requests_arn     = "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${var.name_prefix}-requests"
  executions_arn   = "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${var.name_prefix}-executions"
  coordination_arn = "arn:aws:dynamodb:${var.region}:${local.account_id}:table/${var.name_prefix}-coordination"
  jobs_queue_arn   = "arn:aws:sqs:${var.region}:${local.account_id}:${var.name_prefix}-jobs.fifo"
  results_bucket   = "arn:aws:s3:::${var.name_prefix}-results"
  ecr_prefix_arn   = "arn:aws:ecr:${var.region}:${local.account_id}:repository/${var.name_prefix}-*"
  logs_prefix_arn  = "arn:aws:logs:${var.region}:${local.account_id}:log-group:/ecs/${var.name_prefix}-*:*"
}

# Customer-Managed KMS Key with automatic annual rotation (Constitution §VII).
data "aws_iam_policy_document" "kms_policy" {
  # checkov:skip=CKV_AWS_109:AWS KMS key policy requires Resource="*" which scopes solely to this key
  # checkov:skip=CKV_AWS_111:AWS KMS key policy requires Resource="*" which scopes solely to this key
  # checkov:skip=CKV_AWS_356:AWS KMS key policy requires Resource="*" which scopes solely to this key
  statement {
    sid       = "EnableAccountRootAdmin"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
  }

  statement {
    sid = "AllowCloudWatchLogsAndEvents"
    actions = [
      "kms:Encrypt*",
      "kms:Decrypt*",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:Describe*",
    ]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.region}.amazonaws.com", "sns.amazonaws.com", "sqs.amazonaws.com", "scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_kms_key" "this" {
  description             = "${var.name_prefix} CMK for DynamoDB, SQS, S3, SNS, ECR, and CloudWatch Logs"
  deletion_window_in_days = 14
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.kms_policy.json
  tags                    = var.tags
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name_prefix}"
  target_key_id = aws_kms_key.this.key_id
}

# ECS Task Execution Role (ECR pull + CloudWatch Logs + KMS decrypt).
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "${var.name_prefix}-ecs-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "ecs_execution" {
  statement {
    actions = [
      "ecr:GetAuthorizationToken",
    ]
    resources = ["*"]
  }
  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
    ]
    resources = [local.ecr_prefix_arn]
  }
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [local.logs_prefix_arn]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.this.arn]
  }
}

resource "aws_iam_role_policy" "ecs_execution" {
  name   = "${var.name_prefix}-ecs-execution-policy"
  role   = aws_iam_role.ecs_execution.id
  policy = data.aws_iam_policy_document.ecs_execution.json
}

# 1. Dispatcher Task Role — CANNOT invoke execute-api or read S3 results (006 US4.1).
resource "aws_iam_role" "dispatcher_task" {
  name               = "${var.name_prefix}-dispatcher-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "dispatcher_task" {
  statement {
    sid       = "ReadRequestsSlotIndex"
    actions   = ["dynamodb:Query"]
    resources = ["${local.requests_arn}/index/gsi_slot_shard"]
  }
  statement {
    sid       = "WriteExecutionsPendingAndEnqueued"
    actions   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [local.executions_arn]
  }
  statement {
    sid       = "ManageCoordinationLeaseAndWatermark"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:ConditionCheckItem"]
    resources = [local.coordination_arn]
  }
  statement {
    sid       = "EnqueueFIFOJobs"
    actions   = ["sqs:SendMessage", "sqs:GetQueueUrl"]
    resources = [local.jobs_queue_arn]
  }
  statement {
    sid       = "XRayAndKMS"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.this.arn]
  }
}

resource "aws_iam_role_policy" "dispatcher_task" {
  name   = "${var.name_prefix}-dispatcher-task-policy"
  role   = aws_iam_role.dispatcher_task.id
  policy = data.aws_iam_policy_document.dispatcher_task.json
}

# 2. Worker Task Role — CANNOT touch coordination table (006 US4.1).
resource "aws_iam_role" "worker_task" {
  name               = "${var.name_prefix}-worker-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "worker_task" {
  statement {
    sid       = "ReadCatalogRequest"
    actions   = ["dynamodb:GetItem"]
    resources = [local.requests_arn]
  }
  statement {
    sid       = "UpdateExecutionState"
    actions   = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [local.executions_arn]
  }
  statement {
    sid = "ConsumeFIFOJobs"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueUrl",
      "sqs:GetQueueAttributes",
    ]
    resources = [local.jobs_queue_arn]
  }
  statement {
    sid       = "StoreOversizedResultsS3"
    actions   = ["s3:PutObject"]
    resources = ["${local.results_bucket}/results/*"]
  }
  statement {
    sid       = "InvokePrivateInternalAPI"
    actions   = ["execute-api:Invoke"]
    resources = [var.api_gateway_execution_arn]
  }
  statement {
    sid       = "KMSEncryptDecrypt"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.this.arn]
  }
}

resource "aws_iam_role_policy" "worker_task" {
  name   = "${var.name_prefix}-worker-task-policy"
  role   = aws_iam_role.worker_task.id
  policy = data.aws_iam_policy_document.worker_task.json
}

# 3. Reconciler Task Role (daily coverage + capacity check, 005 + 001-T025).
resource "aws_iam_role" "reconciler_task" {
  name               = "${var.name_prefix}-reconciler-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "reconciler_task" {
  statement {
    sid       = "ReadRequestsSlotIndex"
    actions   = ["dynamodb:Query"]
    resources = ["${local.requests_arn}/index/gsi_slot_shard"]
  }
  statement {
    sid       = "BatchGetAndResetExecutions"
    actions   = ["dynamodb:BatchGetItem", "dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [local.executions_arn]
  }
  statement {
    sid       = "ReadProviderRegistry"
    actions   = ["dynamodb:Query", "dynamodb:GetItem", "dynamodb:Scan"]
    resources = [local.coordination_arn]
  }
  statement {
    sid       = "RedispatchFIFOJobs"
    actions   = ["sqs:SendMessage", "sqs:GetQueueUrl"]
    resources = [local.jobs_queue_arn]
  }
  statement {
    sid       = "WriteDailyReportsS3"
    actions   = ["s3:PutObject"]
    resources = ["${local.results_bucket}/reports/*"]
  }
  statement {
    sid       = "KMSEncryptDecrypt"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.this.arn]
  }
}

resource "aws_iam_role_policy" "reconciler_task" {
  name   = "${var.name_prefix}-reconciler-task-policy"
  role   = aws_iam_role.reconciler_task.id
  policy = data.aws_iam_policy_document.reconciler_task.json
}

# 4. EventBridge Scheduler Role to launch daily reconciler ECS task.
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "scheduler_policy" {
  statement {
    sid       = "RunReconcilerTask"
    actions   = ["ecs:RunTask"]
    resources = ["arn:aws:ecs:${var.region}:${local.account_id}:task-definition/${var.name_prefix}-reconciler:*"]
  }
  statement {
    sid     = "PassExecutionAndReconcilerRoles"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.ecs_execution.arn,
      aws_iam_role.reconciler_task.arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "${var.name_prefix}-scheduler-policy"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_policy.json
}

# 5. GitHub Actions OIDC Deploy Role (no static AWS access keys, 006 US3.4).
resource "aws_iam_openid_connect_provider" "github" {
  count           = var.create_github_oidc_provider ? 1 : 0
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
  tags            = var.tags
}

data "aws_iam_policy_document" "github_oidc_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::${local.account_id}:oidc-provider/token.actions.githubusercontent.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "github_oidc" {
  name               = "${var.name_prefix}-github-oidc-deploy-role"
  assume_role_policy = data.aws_iam_policy_document.github_oidc_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "github_oidc_deploy" {
  statement {
    sid       = "ECRPush"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid = "ECRRepoPush"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecr:BatchGetImage",
    ]
    resources = [local.ecr_prefix_arn]
  }
  statement {
    sid = "ECSDeploy"
    actions = [
      "ecs:DescribeServices",
      "ecs:DescribeTaskDefinition",
      "ecs:RegisterTaskDefinition",
      "ecs:UpdateService",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.region]
    }
  }
  statement {
    sid     = "PassECSRoles"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.ecs_execution.arn,
      aws_iam_role.dispatcher_task.arn,
      aws_iam_role.worker_task.arn,
      aws_iam_role.reconciler_task.arn,
    ]
  }
}

resource "aws_iam_role_policy" "github_oidc_deploy" {
  name   = "${var.name_prefix}-github-oidc-deploy-policy"
  role   = aws_iam_role.github_oidc.id
  policy = data.aws_iam_policy_document.github_oidc_deploy.json
}

# 6. Private API Gateway Resource Policy restricting invocation to aws:SourceVpce + worker role (006-T019).
data "aws_iam_policy_document" "api_gateway_resource_policy" {
  statement {
    sid       = "DenyNonVPCESources"
    effect    = "Deny"
    actions   = ["execute-api:Invoke"]
    resources = [var.api_gateway_execution_arn]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "StringNotEquals"
      variable = "aws:SourceVpce"
      values   = [var.execute_api_vpce_id != "" ? var.execute_api_vpce_id : "vpce-placeholder"]
    }
  }

  statement {
    sid       = "AllowWorkerTaskRoleViaVPCE"
    effect    = "Allow"
    actions   = ["execute-api:Invoke"]
    resources = [var.api_gateway_execution_arn]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.worker_task.arn]
    }
  }
}
