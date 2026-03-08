variable "enable_patchit_failure_pack" {
  type        = bool
  description = "Enable deployment of PATCHIT Snowflake failure SQL pack via Terraform."
  default     = true
}

variable "snowsql_connection_name" {
  type        = string
  description = "snowsql connection/profile name used by Terraform local-exec for failure SQL deployment."
  default     = ""
}
