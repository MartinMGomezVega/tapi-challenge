output "requests_table_name" {
  value = aws_dynamodb_table.requests.name
}

output "requests_table_arn" {
  value = aws_dynamodb_table.requests.arn
}

output "executions_table_name" {
  value = aws_dynamodb_table.executions.name
}

output "executions_table_arn" {
  value = aws_dynamodb_table.executions.arn
}

output "coordination_table_name" {
  value = aws_dynamodb_table.coordination.name
}

output "coordination_table_arn" {
  value = aws_dynamodb_table.coordination.arn
}

output "results_bucket_name" {
  value = aws_s3_bucket.results.bucket
}

output "results_bucket_arn" {
  value = aws_s3_bucket.results.arn
}
