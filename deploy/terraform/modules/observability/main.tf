resource "aws_sns_topic" "alerts" {
  name              = "${var.name_prefix}-alerts"
  kms_master_key_id = var.kms_key_arn
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# 1. DispatchLag (P1) — docs/runbooks/dispatch-lag.md
resource "aws_cloudwatch_metric_alarm" "dispatch_lag" {
  alarm_name          = "${var.name_prefix}-dispatch-lag-high"
  alarm_description   = "P1: DispatchLagMinutes > 5 for 5 consecutive minutes. Runbook: docs/runbooks/dispatch-lag.md"
  namespace           = var.metric_namespace
  metric_name         = "DispatchLagMinutes"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 5
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# 2. QueueAgeHigh (P2) — docs/runbooks/queue-age.md
resource "aws_cloudwatch_metric_alarm" "queue_age_high" {
  alarm_name          = "${var.name_prefix}-queue-age-high"
  alarm_description   = "P2: ApproximateAgeOfOldestMessage > 30 min. Runbook: docs/runbooks/queue-age.md"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 1800
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    QueueName = var.jobs_queue_name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
  tags          = var.tags
}

# 3. DLQNotEmpty (P2) — docs/runbooks/dlq.md
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name_prefix}-dlq-messages-visible"
  alarm_description   = "P2: DLQ FIFO contains visible messages (> 0). Runbook: docs/runbooks/dlq.md"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    QueueName = var.dlq_queue_name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
  tags          = var.tags
}

# 4. CoverageLow (P1) — docs/runbooks/coverage-low.md
resource "aws_cloudwatch_metric_alarm" "coverage_low" {
  alarm_name          = "${var.name_prefix}-daily-coverage-low"
  alarm_description   = "P1: DailyCoverage < 99.9%. Runbook: docs/runbooks/coverage-low.md"
  namespace           = var.metric_namespace
  metric_name         = "DailyCoverage"
  statistic           = "Minimum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 99.9
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# 5. AuthErrors (P1) — docs/runbooks/auth-errors.md
resource "aws_cloudwatch_metric_alarm" "auth_errors" {
  alarm_name          = "${var.name_prefix}-auth-errors"
  alarm_description   = "P1: ExecutionFailed{class=AUTH} > 0. Runbook: docs/runbooks/auth-errors.md"
  namespace           = var.metric_namespace
  metric_name         = "ExecutionFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    class = "AUTH"
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
  tags          = var.tags
}

# 6. LambdaQuotaPressure (P1) — docs/runbooks/lambda-quota.md
resource "aws_cloudwatch_metric_alarm" "lambda_quota_pressure" {
  alarm_name          = "${var.name_prefix}-lambda-quota-pressure"
  alarm_description   = "P1: Account Lambda ConcurrentExecutions > 80% of regional quota. Runbook: docs/runbooks/lambda-quota.md"
  namespace           = "AWS/Lambda"
  metric_name         = "ConcurrentExecutions"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = floor(var.lambda_budget * 0.8)
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# 7. ProviderOverCapacity (P2) — docs/runbooks/provider-capacity.md
resource "aws_cloudwatch_metric_alarm" "provider_over_capacity" {
  alarm_name          = "${var.name_prefix}-provider-over-capacity"
  alarm_description   = "P2: Provider daily volume exceeds 70% sequential budget. Runbook: docs/runbooks/provider-capacity.md"
  namespace           = var.metric_namespace
  metric_name         = "ProviderOverCapacity"
  statistic           = "Maximum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  tags                = var.tags
}

# Operational CloudWatch Dashboard (005 US4, T020)
resource "aws_cloudwatch_dashboard" "operations" {
  dashboard_name = "${var.name_prefix}-operations"
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title = "Dispatcher — Records Dispatched & Lag"
          metrics = [
            [var.metric_namespace, "RecordsDispatched", { stat = "Sum" }],
            [var.metric_namespace, "DispatchLagMinutes", { stat = "Maximum", yAxis = "right" }]
          ]
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title = "Daily Coverage (%) & Missing Executions"
          metrics = [
            [var.metric_namespace, "DailyCoverage", { stat = "Minimum" }],
            [var.metric_namespace, "MissingExecutions", { stat = "Maximum", yAxis = "right" }]
          ]
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title = "Worker — Provider Latency (p50 / p95 / p99)"
          metrics = [
            [var.metric_namespace, "ProviderLatency", { stat = "p50" }],
            [var.metric_namespace, "ProviderLatency", { stat = "p95" }],
            [var.metric_namespace, "ProviderLatency", { stat = "p99" }]
          ]
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title = "Errors, Breakers & SQS FIFO Backlog"
          metrics = [
            [var.metric_namespace, "ExecutionFailed", { stat = "Sum" }],
            [var.metric_namespace, "BreakerOpen", { stat = "Maximum" }],
            ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", var.jobs_queue_name, { stat = "Maximum", yAxis = "right" }],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", var.dlq_queue_name, { stat = "Maximum", yAxis = "right" }]
          ]
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 24
        height = 6
        properties = {
          title = "Account Lambda Concurrency vs Quota"
          metrics = [
            ["AWS/Lambda", "ConcurrentExecutions", { stat = "Maximum" }],
            ["AWS/Lambda", "Throttles", { stat = "Sum", yAxis = "right" }]
          ]
          period = 60
        }
      }
    ]
  })
}

# Glue Catalog Database + Table for Athena querying of S3 daily reports (005 FR-004, T014)
resource "aws_glue_catalog_database" "reports" {
  name = replace("${var.name_prefix}_reports", "-", "_")
}

resource "aws_glue_catalog_table" "reconciliation_daily" {
  name          = "reconciliation_daily"
  database_name = aws_glue_catalog_database.reports.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "projection.enabled"               = "true"
    "projection.partition_date.type"   = "date"
    "projection.partition_date.format" = "yyyy-MM-dd"
    "projection.partition_date.range"  = "2026-01-01,NOW"
    "storage.location.template"        = "s3://${var.results_bucket_name}/reports/run_date=$${partition_date}/"
    "skip.header.line.count"           = "1"
    "classification"                   = "csv"
  }

  partition_keys {
    name = "partition_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.results_bucket_name}/reports/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "OpenCSVSerde"
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
      parameters = {
        separatorChar = ","
      }
    }

    columns {
      name = "run_date"
      type = "string"
    }
    columns {
      name = "provider"
      type = "string"
    }
    columns {
      name = "expected"
      type = "int"
    }
    columns {
      name = "terminal"
      type = "int"
    }
    columns {
      name = "missing"
      type = "int"
    }
    columns {
      name = "stuck"
      type = "int"
    }
    columns {
      name = "in_flight"
      type = "int"
    }
    columns {
      name = "redispatched"
      type = "int"
    }
    columns {
      name = "coverage_percent"
      type = "double"
    }
  }
}
