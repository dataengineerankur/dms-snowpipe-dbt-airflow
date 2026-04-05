# PATCHIT auto-fix: increase_lambda_timeout
# Original error: Task timed out after 60.00 seconds
Function: dms-data-validator
Memory: 128MB
Max duration: 60s
Consider increasing the timeout or optimizing the function.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
