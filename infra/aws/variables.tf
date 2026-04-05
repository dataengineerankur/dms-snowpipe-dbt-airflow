# PATCHIT auto-fix: unknown
# Original error: botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied
Bucket: dms-snowpipe-dev
Key: processed/customers/2026/04/05/data.parquet
Role: GlueServiceRole
variable "aws_region" {
  type        = string
  description = "AWS region."
}

variable "aws_profile" {
  type        = string
  description = "AWS profile to use."
  default     = null
}

variable "project_name" {
  type        = string
  description = "Project name prefix."
}

variable "environment" {
  type        = string
  description = "Environment name (dev, staging, prod)."
  default     = "dev"
}

variable "bucket_name" {
  type        = string
  description = "S3 bucket name for DMS output."
  default     = ""
}

variable "postgres_host" {
  type        = string
  description = "Postgres host accessible by DMS."
  default     = ""
}

variable "postgres_port" {
  type        = number
  description = "Postgres port."
  default     = 5432
}

variable "postgres_db" {
  type        = string
  description = "Postgres database name."
}

variable "postgres_user" {
  type        = string
  description = "Postgres username."
}

variable "postgres_password" {
  type        = string
  description = "Postgres password."
  sensitive   = true
}

variable "use_rds" {
  type        = bool
  description = "Whether to create an RDS Postgres instance."
  default     = true
}

variable "rds_instance_class" {
  type        = string
  description = "RDS instance class."
  default     = "db.t3.micro"
}

variable "rds_allocated_storage_gb" {
  type        = number
  description = "RDS allocated storage in GB."
  default     = 20
}

variable "rds_engine_version" {
  type        = string
  description = "RDS Postgres engine version."
  default     = "14.11"
}

variable "rds_parameter_group_family" {
  type        = string
  description = "RDS parameter group family."
  default     = "postgres15"
}

variable "rds_publicly_accessible" {
  type        = bool
  description = "Whether RDS should be publicly accessible."
  default     = true
}

variable "rds_allowed_cidrs" {
  type        = list(string)
  description = "CIDR blocks allowed to access RDS (for psql from your laptop)."
  default     = []
}

variable "enable_redshift" {
  type        = bool
  description = "Whether to provision a Redshift cluster and Glue job."
  default     = true
}

variable "enable_glue_athena" {
  type        = bool
  description = "Whether to provision Glue Catalog/Crawlers and Athena workgroup."
  default     = true
}

variable "enable_redshift_serverless" {
  type        = bool
  description = "Whether to provision Redshift Serverless."
  default     = false
}

variable "redshift_cluster_identifier" {
  type        = string
  description = "Redshift cluster identifier."
  default     = "dms-snowpipe-redshift"
}

variable "redshift_database" {
  type        = string
  description = "Redshift database name."
  default     = "analytics"
}

variable "redshift_master_username" {
  type        = string
  description = "Redshift master username."
  default     = "admin"
}

variable "redshift_master_password" {
  type        = string
  description = "Redshift master password."
  sensitive   = true
}

variable "redshift_node_type" {
  type        = string
  description = "Redshift node type."
  default     = "dc2.large"
}

variable "redshift_publicly_accessible" {
  type        = bool
  description = "Whether Redshift is publicly accessible."
  default     = true
}

variable "redshift_allowed_cidrs" {
  type        = list(string)
  description = "CIDR blocks allowed to access Redshift."
  default     = []
}

variable "redshift_serverless_namespace" {
  type        = string
  description = "Redshift Serverless namespace name."
  default     = "dms-redshift-namespace"
}

variable "redshift_serverless_workgroup" {
  type        = string
  description = "Redshift Serverless workgroup name."
  default     = "dms-redshift-workgroup"
}

variable "redshift_serverless_base_capacity" {
  type        = number
  description = "Redshift Serverless base capacity in RPUs."
  default     = 8
}

variable "glue_job_name" {
  type        = string
  description = "Glue job name for Redshift loads."
  default     = "dms-glue-redshift-load"
}

variable "glue_silver_job_name" {
  type        = string
  description = "Glue job name for legacy silver transforms."
  default     = "dms-glue-silver-transform"
}

variable "glue_gold_job_name" {
  type        = string
  description = "Glue job name for legacy gold transforms."
  default     = "dms-glue-gold-transform"
}

variable "glue_silver_customers_job_name" {
  type        = string
  description = "Glue job name for silver customers transform."
  default     = "dms-glue-silver-customers"
}

variable "glue_silver_products_job_name" {
  type        = string
  description = "Glue job name for silver products transform."
  default     = "dms-glue-silver-products"
}

variable "glue_silver_orders_job_name" {
  type        = string
  description = "Glue job name for silver orders transform."
  default     = "dms-glue-silver-orders"
}

variable "glue_gold_customers_job_name" {
  type        = string
  description = "Glue job name for gold customers transform."
  default     = "dms-glue-gold-customers"
}

variable "glue_gold_products_job_name" {
  type        = string
  description = "Glue job name for gold products transform."
  default     = "dms-glue-gold-products"
}

variable "glue_gold_orders_job_name" {
  type        = string
  description = "Glue job name for gold orders transform."
  default     = "dms-glue-gold-orders"
}

variable "glue_raw_ingest_job_name" {
  type        = string
  description = "Glue job name to ingest from dms_lake into raw."
  default     = "dms-glue-raw-ingest"
}

variable "glue_worker_type" {
  type        = string
  description = "Glue worker type."
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  type        = number
  description = "Glue number of workers."
  default     = 2
}

variable "glue_timeout_minutes" {
  type        = number
  description = "Glue job timeout in minutes."
  default     = 30
}

variable "glue_catalog_database_raw" {
  type        = string
  description = "Glue Catalog database name for DMS raw data (landing)."
  default     = "dms_datalake"
}

variable "glue_catalog_database_bronze" {
  type        = string
  description = "Glue Catalog database name for bronze (raw_ingest output)."
  default     = "bronze"
}

variable "glue_catalog_database_silver" {
  type        = string
  description = "Glue Catalog database name for silver."
  default     = "silver"
}

variable "glue_catalog_database_gold" {
  type        = string
  description = "Glue Catalog database name for gold."
  default     = "gold"
}

variable "glue_crawler_raw_name" {
  type        = string
  description = "Glue crawler name for DMS raw data."
  default     = "dms-raw-crawler"
}

variable "glue_crawler_silver_name" {
  type        = string
  description = "Glue crawler name for silver data."
  default     = "dms-silver-crawler"
}

variable "glue_crawler_gold_name" {
  type        = string
  description = "Glue crawler name for gold data."
  default     = "dms-gold-crawler"
}

variable "glue_crawler_bronze_name" {
  type        = string
  description = "Glue crawler name for bronze data."
  default     = "dms-bronze-crawler"
