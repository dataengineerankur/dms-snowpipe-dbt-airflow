# Workspace already has a Default Storage managed catalog (e.g. patchit_demo2 for workspace patchit-demo2).
# Point var.catalog at that name — do not create a separate catalog without an explicit storage_root.

resource "databricks_schema" "lakehouse" {
  catalog_name = var.catalog
  name         = var.schema
  comment      = "Bronze, silver, and gold Delta tables for DMS domains"
}

resource "databricks_volume" "pipeline_checkpoints" {
  name         = "pipeline_checkpoints"
  catalog_name = var.catalog
  schema_name  = var.schema
  volume_type  = "MANAGED"
  comment      = "Auto Loader checkpoints and CloudFiles schema storage for DMS pipelines"
}
