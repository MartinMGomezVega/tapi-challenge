resource "aws_sqs_queue" "dlq" {
  name                        = "${var.name_prefix}-jobs-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  message_retention_seconds   = 1209600 # 14 days
  kms_master_key_id           = var.kms_key_arn
  tags                        = var.tags
}

resource "aws_sqs_queue" "jobs" {
  name                        = "${var.name_prefix}-jobs.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  deduplication_scope         = "messageGroup"
  fifo_throughput_limit       = "perMessageGroupId"
  visibility_timeout_seconds  = var.visibility_timeout_seconds
  receive_wait_time_seconds   = 20
  message_retention_seconds   = 345600 # 4 days
  kms_master_key_id           = var.kms_key_arn

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  tags = var.tags
}
