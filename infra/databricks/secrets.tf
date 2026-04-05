# Optional AWS keys for S3 reads (Auto Loader). Pass only at apply time:
#   TF_VAR_s3_aws_access_key_id=... TF_VAR_s3_aws_secret_access_key=... terraform apply

variable "s3_aws_access_key_id" {
  type        = string
  sensitive   = true
  default     = ""
  description = "Written to secret scope as aws_access_key_name when set together with s3_aws_secret_access_key."
}

variable "s3_aws_secret_access_key" {
  type        = string
  sensitive   = true
  default     = ""
  description = "Written to secret scope as aws_secret_key_name when set together with s3_aws_access_key_id."
}

locals {
  s3_secrets_ready = trimspace(var.s3_aws_access_key_id) != "" && trimspace(var.s3_aws_secret_access_key) != ""
  s3_scope_name    = trimspace(var.aws_secret_scope) != "" ? trimspace(var.aws_secret_scope) : "dms_s3"
}

resource "databricks_secret_scope" "s3" {
  name                     = local.s3_scope_name
  initial_manage_principal = "users"
}

resource "databricks_secret" "s3_access_key_id" {
  count        = local.s3_secrets_ready ? 1 : 0
  scope        = databricks_secret_scope.s3.name
  key          = var.aws_access_key_name
  string_value = var.s3_aws_access_key_id
}

resource "databricks_secret" "s3_secret_access_key" {
  count        = local.s3_secrets_ready ? 1 : 0
  scope        = databricks_secret_scope.s3.name
  key          = var.aws_secret_key_name
  string_value = var.s3_aws_secret_access_key
}
