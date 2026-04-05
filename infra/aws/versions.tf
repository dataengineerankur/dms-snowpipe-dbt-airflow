# PATCHIT auto-fix: unknown
# Original error: botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied
Bucket: dms-snowpipe-dev
Key: processed/customers/2026/04/05/data.parquet
Role: GlueServiceRole
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
