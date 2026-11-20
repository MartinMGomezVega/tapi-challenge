#!/bin/sh
set -eu

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

aws_cmd() {
  aws --endpoint-url="$ENDPOINT" --region="$REGION" "$@"
}

echo "[init] Ensuring DynamoDB table: requests"
if ! aws_cmd dynamodb describe-table --table-name requests >/dev/null 2>&1; then
  aws_cmd dynamodb create-table \
    --table-name requests \
    --billing-mode PAY_PER_REQUEST \
    --attribute-definitions \
      AttributeName=id,AttributeType=S \
      AttributeName=slot_shard,AttributeType=S \
      AttributeName=provider_id,AttributeType=S \
    --key-schema \
      AttributeName=id,KeyType=HASH \
    --global-secondary-indexes '[
      {
        "IndexName": "gsi_slot_shard",
        "KeySchema": [
          {"AttributeName": "slot_shard", "KeyType": "HASH"},
          {"AttributeName": "provider_id", "KeyType": "RANGE"}
        ],
        "Projection": {
          "ProjectionType": "INCLUDE",
          "NonKeyAttributes": ["provider", "enabled", "created_at"]
        }
      }
    ]' >/dev/null
  aws_cmd dynamodb wait table-exists --table-name requests
fi

echo "[init] Ensuring DynamoDB table: executions"
if ! aws_cmd dynamodb describe-table --table-name executions >/dev/null 2>&1; then
  aws_cmd dynamodb create-table \
    --table-name executions \
    --billing-mode PAY_PER_REQUEST \
    --attribute-definitions \
      AttributeName=request_id,AttributeType=S \
      AttributeName=run_date,AttributeType=S \
    --key-schema \
      AttributeName=request_id,KeyType=HASH \
      AttributeName=run_date,KeyType=RANGE >/dev/null
  aws_cmd dynamodb wait table-exists --table-name executions
  aws_cmd dynamodb update-time-to-live \
    --table-name executions \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null || true
fi

echo "[init] Ensuring DynamoDB table: coordination"
if ! aws_cmd dynamodb describe-table --table-name coordination >/dev/null 2>&1; then
  aws_cmd dynamodb create-table \
    --table-name coordination \
    --billing-mode PAY_PER_REQUEST \
    --attribute-definitions \
      AttributeName=name,AttributeType=S \
    --key-schema \
      AttributeName=name,KeyType=HASH >/dev/null
  aws_cmd dynamodb wait table-exists --table-name coordination
fi

echo "[init] Ensuring SQS FIFO DLQ: tapi-jobs-dlq.fifo"
DLQ_URL=$(aws_cmd sqs create-queue \
  --queue-name tapi-jobs-dlq.fifo \
  --attributes \
    FifoQueue=true,ContentBasedDeduplication=false,DeduplicationScope=messageGroup,FifoThroughputLimit=perMessageGroupId \
  --query 'QueueUrl' --output text)

DLQ_ARN=$(aws_cmd sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

echo "[init] Ensuring SQS FIFO queue: tapi-jobs.fifo (maxReceiveCount=6)"
QUEUE_ATTRS=$(printf '{"FifoQueue":"true","ContentBasedDeduplication":"false","DeduplicationScope":"messageGroup","FifoThroughputLimit":"perMessageGroupId","VisibilityTimeout":"60","RedrivePolicy":"{\\"deadLetterTargetArn\\":\\"%s\\",\\"maxReceiveCount\\":\\"6\\"}"}' "$DLQ_ARN")
aws_cmd sqs create-queue \
  --queue-name tapi-jobs.fifo \
  --attributes "$QUEUE_ATTRS" \
  >/dev/null

echo "[init] Ensuring S3 bucket: tapi-results"
if ! aws_cmd s3api head-bucket --bucket tapi-results >/dev/null 2>&1; then
  aws_cmd s3api create-bucket --bucket tapi-results >/dev/null
fi

echo "[init] LocalStack initialization complete."
