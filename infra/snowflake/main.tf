# PATCHIT auto-fix: grant_permissions
# Original error: Insufficient privileges to operate on warehouse COMPUTE_WH. GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOADER_ROLE
# PATCHIT auto-fix: grant_permissions
# Original error: SQL access control error: Insufficient privileges to operate on warehouse COMPUTE_WH
Role: LOADER_ROLE
Required: USAGE on WAREHOUSE COMPUTE_WH
File: infra/snowflake/main.tf
Fix: GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOADER_ROLE
locals {
  db_name         = var.database_name
  raw_schema      = "RAW"
  stg_schema      = "STG"
  int_schema      = "INT"
  gold_schema     = "GOLD"
  ingest_role     = "INGEST_ROLE"
  transform_role  = "TRANSFORM_ROLE"
  ingest_wh       = "INGEST_WH"
  transform_wh    = "TRANSFORM_WH"
  stage_name      = "DMS_STAGE"
  file_format     = "DMS_PARQUET_FF"
  storage_integ   = "S3_DMS_INT"
}

resource "snowflake_database" "analytics" {
  name = local.db_name
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.analytics.name
  name     = local.raw_schema
}

resource "snowflake_schema" "stg" {
  database = snowflake_database.analytics.name
  name     = local.stg_schema
}

resource "snowflake_schema" "int" {
  database = snowflake_database.analytics.name
  name     = local.int_schema
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.analytics.name
  name     = local.gold_schema
}

resource "snowflake_warehouse" "ingest" {
  name                = local.ingest_wh
  warehouse_size      = var.ingest_wh_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_warehouse" "transform" {
  name                = local.transform_wh
  warehouse_size      = var.transform_wh_size
  auto_suspend        = 120
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_role" "ingest" {
  name = local.ingest_role
}

resource "snowflake_role" "transform" {
  name = local.transform_role
}

resource "snowflake_database_grant" "db_usage_ingest" {
  database_name = snowflake_database.analytics.name
  privilege     = "USAGE"
  roles         = [snowflake_role.ingest.name]
}

resource "snowflake_database_grant" "db_usage_transform" {
  database_name = snowflake_database.analytics.name
  privilege     = "USAGE"
  roles         = [snowflake_role.transform.name]
}

resource "snowflake_schema_grant" "raw_usage_ingest" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.raw.name
  privilege     = "USAGE"
  roles         = [snowflake_role.ingest.name]
}

resource "snowflake_schema_grant" "raw_usage_transform" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.raw.name
  privilege     = "USAGE"
  roles         = [snowflake_role.transform.name]
}

resource "snowflake_schema_grant" "stg_usage_transform" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.stg.name
  privilege     = "USAGE"
  roles         = [snowflake_role.transform.name]
}

resource "snowflake_schema_grant" "int_usage_transform" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.int.name
  privilege     = "USAGE"
  roles         = [snowflake_role.transform.name]
}

resource "snowflake_schema_grant" "gold_usage_transform" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.gold.name
  privilege     = "USAGE"
  roles         = [snowflake_role.transform.name]
}

resource "snowflake_warehouse_grant" "ingest_wh_usage" {
  warehouse_name = snowflake_warehouse.ingest.name
  privilege      = "USAGE"
  roles          = [snowflake_role.ingest.name]
}

resource "snowflake_warehouse_grant" "transform_wh_usage" {
  warehouse_name = snowflake_warehouse.transform.name
  privilege      = "USAGE"
  roles          = [snowflake_role.transform.name]
}

resource "snowflake_storage_integration" "s3" {
  name                      = local.storage_integ
  storage_provider          = "S3"
  enabled                   = true
  storage_aws_role_arn       = var.storage_integration_role_arn
  storage_allowed_locations  = ["s3://${var.s3_bucket_name}/dms/"]
}

resource "snowflake_file_format" "parquet" {
  name     = local.file_format
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.raw.name
  format_type = "PARQUET"
}

resource "snowflake_stage" "dms" {
  name                = local.stage_name
  database            = snowflake_database.analytics.name
  schema              = snowflake_schema.raw.name
  url                 = "s3://${var.s3_bucket_name}/dms/"
  storage_integration = snowflake_storage_integration.s3.name

  lifecycle {
    ignore_changes = [file_format]
  }
}

resource "snowflake_stage_grant" "dms_usage" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.raw.name
  stage_name    = snowflake_stage.dms.name
  privilege     = "USAGE"
  roles         = [snowflake_role.ingest.name]
}

resource "snowflake_table" "raw_customers" {
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.raw.name
  name     = "CUSTOMERS"

  column {
    name = "CUSTOMER_ID"
    type = "NUMBER"
  }
  column {
    name = "FIRST_NAME"
    type = "VARCHAR"
  }
  column {
    name = "LAST_NAME"
    type = "VARCHAR"
  }
  column {
    name = "EMAIL"
    type = "VARCHAR"
  }
  column {
    name = "CREATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "DMS_OP"
    type = "VARCHAR"
  }
  column {
    name = "DMS_COMMIT_TS"
    type = "TIMESTAMP_LTZ"
  }
  column {
    name = "DMS_LOAD_TS"
    type = "TIMESTAMP_LTZ"
  }
  column {
    name = "DMS_FILE_NAME"
    type = "VARCHAR"
  }
}

resource "snowflake_table" "raw_products" {
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.raw.name
  name     = "PRODUCTS"

  column {
    name = "PRODUCT_ID"
    type = "NUMBER"
  }
  column {
    name = "SKU"
    type = "VARCHAR"
  }
  column {
    name = "PRODUCT_NAME"
    type = "VARCHAR"
  }
  column {
    name = "CATEGORY"
    type = "VARCHAR"
  }
  column {
    name = "PRICE"
    type = "NUMBER(12,2)"
  }
  column {
    name = "CREATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "DMS_OP"
    type = "VARCHAR"
  }
  column {
    name = "DMS_COMMIT_TS"
    type = "TIMESTAMP_LTZ"
  }
  column {
    name = "DMS_LOAD_TS"
    type = "TIMESTAMP_LTZ"
  }
  column {
    name = "DMS_FILE_NAME"
    type = "VARCHAR"
  }
}

resource "snowflake_table" "raw_orders" {
  database = snowflake_database.analytics.name
  schema   = snowflake_schema.raw.name
  name     = "ORDERS"

  column {
    name = "ORDER_ID"
    type = "NUMBER"
  }
  column {
    name = "CUSTOMER_ID"
    type = "NUMBER"
  }
  column {
    name = "ORDER_STATUS"
    type = "VARCHAR"
  }
  column {
    name = "ORDER_DATE"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "DMS_OP"
    type = "VARCHAR"
  }
  column {
    name = "DMS_COMMIT_TS"
    type = "TIMESTAMP_LTZ"
  }
  column {
    name = "DMS_LOAD_TS"
    type = "TIMESTAMP_LTZ"
  }
  column {
    name = "DMS_FILE_NAME"
