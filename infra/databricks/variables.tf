variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL, e.g. https://adb-1234567890123456.7.azuredatabricks.net"
}

variable "databricks_token" {
  type        = string
  description = "Databricks PAT (optional if databricks_azure_workspace_resource_id is set and you use az login)."
  sensitive   = true
  default     = ""
}

variable "databricks_azure_workspace_resource_id" {
  type        = string
  description = "ARM resource ID of the Azure Databricks workspace; enables Azure CLI/AAD auth when set."
  default     = ""
}

variable "job_cluster_id" {
  type        = string
  description = "Legacy existing-cluster id (unused when job clusters are enabled)."
  default     = ""
}

variable "repo_url" {
  type        = string
  description = "Git URL of this repository."
  default     = ""
}

variable "repo_provider" {
  type        = string
  description = "Git provider value for Databricks jobs."
  default     = "gitHub"
}

variable "repo_branch" {
  type        = string
  description = "Git branch to run notebooks from."
  default     = "main"
}

variable "repo_path" {
  type        = string
  description = "Workspace repo path to use for jobs (e.g., /Users/<email>/dms-snowpipe-dbt-airflow). Leave empty to use workspace notebooks."
  default     = ""
}

variable "workspace_notebook_base_path" {
  type        = string
  description = "Workspace path where Terraform uploads Databricks notebooks."
  default     = "/Shared/dms-snowpipe-dbt-airflow/notebooks"
}

variable "run_as_user" {
  type        = string
  description = "Run-as user for Databricks jobs (must have access to repo/path)."
  default     = ""
}

variable "catalog" {
  type        = string
  description = "Unity Catalog name. On Azure with Default Storage, use the workspace managed catalog (often <workspace_name> with hyphens replaced by underscores), e.g. patchit_demo2."
  default     = "patchit_demo2"
}

variable "schema" {
  type        = string
  description = "Databricks schema/database for bronze/silver/gold tables."
  default     = "walmart_lakehouse"
}

variable "s3_bucket" {
  type        = string
  description = "S3 bucket containing DMS data."
  default     = "dms-snowpipe-dev-05d6e64a"
}

variable "aws_secret_scope" {
  type        = string
  description = "Databricks secret scope name that stores AWS credentials."
  default     = ""
}

variable "aws_access_key_name" {
  type        = string
  description = "Secret key name for AWS access key id."
  default     = "AWS_ACCESS_KEY_ID"
}

variable "aws_secret_key_name" {
  type        = string
  description = "Secret key name for AWS secret access key."
  default     = "AWS_SECRET_ACCESS_KEY"
}

variable "aws_session_token_name" {
  type        = string
  description = "Secret key name for AWS session token (optional)."
  default     = "AWS_SESSION_TOKEN"
}


variable "job_spark_version" {
  type        = string
  description = "Databricks runtime version for job clusters."
  default     = "13.3.x-scala2.12"
}

variable "job_node_type_id" {
  type        = string
  description = "Azure VM SKU for job clusters (single-node). Use the smallest SKU your workspace lists (e.g. Standard_D2ds_v6 for 2 vCPU); D2s_v5 is not enabled on all workspaces. If quota is tight, stop other compute or raise UK West vCPU quota."
  default     = "Standard_D2ds_v6"
}

variable "job_autotermination_minutes" {
  type        = number
  description = "Auto-termination for job clusters."
  default     = 20
}
