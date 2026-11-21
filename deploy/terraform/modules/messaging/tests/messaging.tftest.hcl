mock_provider "aws" {}

variables {
  name_prefix = "tapi-test"
  kms_key_arn = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
}

run "verify_fifo_high_throughput_and_dlq_max_receive_6" {
  command = apply

  assert {
    condition     = aws_sqs_queue.jobs.fifo_queue == true && aws_sqs_queue.dlq.fifo_queue == true
    error_message = "both main queue and DLQ must be FIFO queues"
  }

  assert {
    condition     = aws_sqs_queue.jobs.deduplication_scope == "messageGroup" && aws_sqs_queue.jobs.fifo_throughput_limit == "perMessageGroupId"
    error_message = "main FIFO queue must enable high-throughput perMessageGroupId mode"
  }

  assert {
    condition     = aws_sqs_queue.jobs.visibility_timeout_seconds == 60 && aws_sqs_queue.jobs.receive_wait_time_seconds == 20
    error_message = "main FIFO queue must use 60s visibility timeout and 20s long polling"
  }

  assert {
    condition     = var.max_receive_count == 6 && aws_sqs_queue.jobs.kms_master_key_id == var.kms_key_arn
    error_message = "main FIFO queue must enforce maxReceiveCount=6 and KMS encryption"
  }
}
