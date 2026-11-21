output "alerts_topic_arn" {
  value       = aws_sns_topic.alerts.arn
  description = "ARN of the operational SNS alerts topic."
}

output "dashboard_name" {
  value       = aws_cloudwatch_dashboard.operations.dashboard_name
  description = "CloudWatch operational dashboard name."
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.reports.name
  description = "AWS Glue database name for Athena queries."
}
