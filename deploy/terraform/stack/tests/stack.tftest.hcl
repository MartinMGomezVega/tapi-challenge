mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "111122223333"
    }
  }
  mock_data "aws_servicequotas_service_quota" {
    defaults = {
      value = 1000
    }
  }
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  env                       = "dev"
  api_gateway_execution_arn = "arn:aws:execute-api:us-east-1:111122223333:devapi/v1/*"
  provider_api_base_url     = "https://devapi.execute-api.us-east-1.amazonaws.com/v1"
  inflight_per_task         = 32
  worker_min_tasks          = 2
}

run "verify_full_stack_lambda_budget_and_circuit_breaker" {
  command = plan

  assert {
    condition     = local.lambda_budget == 1000
    error_message = "lambda_budget must derive from Service Quotas L-B99A9384 (1000)"
  }

  assert {
    condition     = module.compute.worker_max_tasks == 15
    error_message = "worker_max_tasks must equal floor(1000 / (2 * 32)) = 15"
  }
}
