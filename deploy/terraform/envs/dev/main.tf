terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }

  backend "s3" {
    bucket       = "tapi-terraform-state-dev"
    key          = "scheduled-queries/dev/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "vpc_cidr" {
  type = string
}

variable "shards" {
  type = number
}

variable "inflight_per_task" {
  type = number
}

variable "worker_min_tasks" {
  type = number
}

variable "api_gateway_execution_arn" {
  type = string
}

variable "provider_api_base_url" {
  type = string
}

module "stack" {
  source                    = "../../stack"
  env                       = "dev"
  region                    = var.region
  vpc_cidr                  = var.vpc_cidr
  shards                    = var.shards
  inflight_per_task         = var.inflight_per_task
  worker_min_tasks          = var.worker_min_tasks
  image_tag                 = var.image_tag
  api_gateway_execution_arn = var.api_gateway_execution_arn
  provider_api_base_url     = var.provider_api_base_url
}
