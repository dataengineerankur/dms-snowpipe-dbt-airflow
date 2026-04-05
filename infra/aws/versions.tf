# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
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
