region                    = "us-east-1"
vpc_cidr                  = "10.20.0.0/16"
shards                    = 4
inflight_per_task         = 16
worker_min_tasks          = 2
api_gateway_execution_arn = "arn:aws:execute-api:us-east-1:111122223333:devapi/v1/*"
provider_api_base_url     = "https://devapi.execute-api.us-east-1.amazonaws.com/v1"
