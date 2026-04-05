terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "databricks" {
  host                        = var.databricks_host
  token                       = var.databricks_token
  azure_workspace_resource_id = var.databricks_azure_workspace_resource_id
}
