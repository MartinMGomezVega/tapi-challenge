region                    = "us-east-1"
vpc_cidr                  = "10.22.0.0/16"
shards                    = 8
inflight_per_task         = 32
worker_min_tasks          = 4
api_gateway_execution_arn = "arn:aws:execute-api:us-east-1:111122223333:prodapi/v1/*"
provider_api_base_url     = "https://prodapi.execute-api.us-east-1.amazonaws.com/v1"
