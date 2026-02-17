variable "snowflake_account" {
  type        = string
  description = "Snowflake account locator."
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake user."
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password."
  sensitive   = true
}

variable "snowflake_role" {
  type        = string
  description = "Snowflake role to use for provisioning."
  default     = "SYSADMIN"
}

variable "database_name" {
  type        = string
  description = "Snowflake database name."
  default     = "ANALYTICS"
}

variable "s3_bucket_name" {
  type        = string
  description = "S3 bucket used for DMS landing."
}

variable "storage_integration_role_arn" {
  type        = string
  description = "AWS IAM role ARN for Snowflake storage integration."
}

variable "ingest_wh_size" {
  type        = string
  description = "Warehouse size for ingestion."
  default     = "XSMALL"
}

variable "transform_wh_size" {
  type        = string
  description = "Warehouse size for transforms."
  default     = "SMALL"
}
