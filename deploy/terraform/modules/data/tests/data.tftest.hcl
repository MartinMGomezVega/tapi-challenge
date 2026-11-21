mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  name_prefix = "tapi-test"
  kms_key_arn = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
}

run "verify_data_pitr_ttl_kms_gsis_and_s3_security" {
  command = apply

  assert {
    condition     = aws_dynamodb_table.requests.billing_mode == "PAY_PER_REQUEST"
    error_message = "requests table must use PAY_PER_REQUEST"
  }

  assert {
    condition     = aws_dynamodb_table.requests.point_in_time_recovery[0].enabled == true
    error_message = "requests table must enable PITR"
  }

  assert {
    condition     = length(aws_dynamodb_table.requests.global_secondary_index) == 2
    error_message = "requests table must define gsi_slot_shard and gsi_provider"
  }

  assert {
    condition     = aws_dynamodb_table.executions.ttl[0].enabled == true && aws_dynamodb_table.executions.ttl[0].attribute_name == "ttl"
    error_message = "executions table must enable TTL on attribute 'ttl'"
  }

  assert {
    condition     = aws_dynamodb_table.executions.server_side_encryption[0].enabled == true
    error_message = "executions table must enable KMS SSE"
  }

  assert {
    condition     = aws_dynamodb_table.coordination.point_in_time_recovery[0].enabled == true
    error_message = "coordination table must enable PITR"
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.results.block_public_acls == true && aws_s3_bucket_public_access_block.results.restrict_public_buckets == true
    error_message = "S3 results bucket must block all public access"
  }
}
