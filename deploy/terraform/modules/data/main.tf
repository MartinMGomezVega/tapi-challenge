# 1. requests catalog table (001 FR-001, FR-004, FR-004b)
resource "aws_dynamodb_table" "requests" {
  name         = "${var.name_prefix}-requests"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }
  attribute {
    name = "provider"
    type = "S"
  }
  attribute {
    name = "slot_shard"
    type = "S"
  }
  attribute {
    name = "provider_id"
    type = "S"
  }

  global_secondary_index {
    name               = "gsi_slot_shard"
    hash_key           = "slot_shard"
    range_key          = "provider_id"
    projection_type    = "INCLUDE"
    non_key_attributes = ["provider", "enabled", "created_at"]
  }

  global_secondary_index {
    name               = "gsi_provider"
    hash_key           = "provider"
    range_key          = "id"
    projection_type    = "INCLUDE"
    non_key_attributes = ["version", "slot", "shard", "enabled"]
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = var.tags
}

# 2. executions daily state & results table (002, 003 FR-004, 005)
resource "aws_dynamodb_table" "executions" {
  name         = "${var.name_prefix}-executions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "request_id"
  range_key    = "run_date"

  attribute {
    name = "request_id"
    type = "S"
  }
  attribute {
    name = "run_date"
    type = "S"
  }
  attribute {
    name = "run_date_status"
    type = "S"
  }
  attribute {
    name = "provider_request_id"
    type = "S"
  }

  global_secondary_index {
    name            = "gsi_run_date_status"
    hash_key        = "run_date_status"
    range_key       = "provider_request_id"
    projection_type = "KEYS_ONLY"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = var.tags
}

# 3. coordination table: leader lease, watermark, provider histograms, backfill checkpoints
resource "aws_dynamodb_table" "coordination" {
  name         = "${var.name_prefix}-coordination"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "name"

  attribute {
    name = "name"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = var.tags
}

# 4. S3 bucket for oversized (> 350 KB) results and daily reconciliation reports
resource "aws_s3_bucket" "results" {
  # checkov:skip=CKV_AWS_144:Single-region deployment required by constraint C2
  # checkov:skip=CKV_AWS_18:Access logging deferred to centralized org audit bucket
  # checkov:skip=CKV2_AWS_62:Event notifications not required for internal report storage
  bucket = "${var.name_prefix}-results"
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "results" {
  bucket = aws_s3_bucket.results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id

  rule {
    id     = "abort-incomplete-multipart-all"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-results"
    status = "Enabled"

    filter {
      prefix = "results/"
    }

    expiration {
      days = var.results_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

data "aws_iam_policy_document" "results_tls_only" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [
      aws_s3_bucket.results.arn,
      "${aws_s3_bucket.results.arn}/*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "results" {
  bucket = aws_s3_bucket.results.id
  policy = data.aws_iam_policy_document.results_tls_only.json
}
