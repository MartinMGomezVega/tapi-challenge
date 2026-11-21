output "jobs_queue_name" {
  value = aws_sqs_queue.jobs.name
}

output "jobs_queue_url" {
  value = aws_sqs_queue.jobs.url
}

output "jobs_queue_arn" {
  value = aws_sqs_queue.jobs.arn
}

output "dlq_queue_name" {
  value = aws_sqs_queue.dlq.name
}

output "dlq_queue_url" {
  value = aws_sqs_queue.dlq.url
}

output "dlq_queue_arn" {
  value = aws_sqs_queue.dlq.arn
}
