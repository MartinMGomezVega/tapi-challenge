region                    = "us-east-1"
vpc_cidr                  = "10.21.0.0/16"
shards                    = 8
inflight_per_task         = 32
worker_min_tasks          = 2
api_gateway_execution_arn = "arn:aws:execute-api:us-east-1:111122223333:stgapi/v1/*"
provider_api_base_url     = "https://stgapi.execute-api.us-east-1.amazonaws.com/v1"
