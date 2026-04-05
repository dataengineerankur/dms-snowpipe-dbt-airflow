locals {
  jobs = {
    customers = {
      gold_notebook_params = {
        domain                 = "customers"
        catalog                = var.catalog
        schema                 = var.schema
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
      base_notebook_params = {
        domain                 = "customers"
        catalog                = var.catalog
        schema                 = var.schema
        bucket                 = var.s3_bucket
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
    }
    products = {
      gold_notebook_params = {
        domain                 = "products"
        catalog                = var.catalog
        schema                 = var.schema
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
      base_notebook_params = {
        domain                 = "products"
        catalog                = var.catalog
        schema                 = var.schema
        bucket                 = var.s3_bucket
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
    }
    orders = {
      gold_notebook_params = {
        domain                 = "orders"
        catalog                = var.catalog
        schema                 = var.schema
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
      base_notebook_params = {
        domain                 = "orders"
        catalog                = var.catalog
        schema                 = var.schema
        bucket                 = var.s3_bucket
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
    }
  }

  notebook_path_bronze = databricks_notebook.bronze_autoloader.path
  notebook_path_silver = databricks_notebook.silver_transform.path
  notebook_path_gold   = databricks_notebook.gold_transform.path
}

resource "databricks_notebook" "bronze_autoloader" {
  path     = "${var.workspace_notebook_base_path}/bronze_autoloader"
  language = "PYTHON"
  source   = "${path.module}/../../databricks/notebooks/bronze_autoloader.py"
}

resource "databricks_notebook" "silver_transform" {
  path     = "${var.workspace_notebook_base_path}/silver_transform"
  language = "PYTHON"
  source   = "${path.module}/../../databricks/notebooks/silver_transform.py"
}

resource "databricks_notebook" "gold_transform" {
  path     = "${var.workspace_notebook_base_path}/gold_transform"
  language = "PYTHON"
  source   = "${path.module}/../../databricks/notebooks/gold_transform.py"
}

resource "databricks_notebook" "retail_silver_transform" {
  path     = "${var.workspace_notebook_base_path}/retail_silver_transform"
  language = "PYTHON"
  source   = "${path.module}/../../databricks/notebooks/retail_silver_transform.py"
}

resource "databricks_notebook" "retail_gold_transform" {
  path     = "${var.workspace_notebook_base_path}/retail_gold_transform"
  language = "PYTHON"
  source   = "${path.module}/../../databricks/notebooks/retail_gold_transform.py"
}

resource "databricks_job" "pipelines" {
  for_each            = local.jobs
  depends_on          = [databricks_schema.lakehouse]
  name                = "${each.key}_pipeline"
  max_concurrent_runs = 1

  dynamic "run_as" {
    for_each = var.run_as_user != "" ? [1] : []
    content {
      user_name = var.run_as_user
    }
  }

  job_cluster {
    job_cluster_key = "patchit_job_cluster"
    new_cluster {
      spark_version      = var.job_spark_version
      node_type_id       = var.job_node_type_id
      num_workers        = 0
      data_security_mode = "USER_ISOLATION"
      azure_attributes {
        availability = "ON_DEMAND_AZURE"
      }
      spark_conf = merge(
        {
          "spark.databricks.cluster.profile" = "singleNode"
          "spark.master"                     = "local[*]"
        },
        trimspace(var.aws_secret_scope) != "" ? {
          "spark.hadoop.fs.s3.awsAccessKeyId"            = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_access_key_name}}}"
          "spark.hadoop.fs.s3.awsSecretAccessKey"        = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_secret_key_name}}}"
          "spark.hadoop.fs.s3n.awsAccessKeyId"           = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_access_key_name}}}"
          "spark.hadoop.fs.s3n.awsSecretAccessKey"       = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_secret_key_name}}}"
          "spark.hadoop.fs.s3a.access.key"               = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_access_key_name}}}"
          "spark.hadoop.fs.s3a.secret.key"               = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_secret_key_name}}}"
          "spark.hadoop.fs.s3a.endpoint"                 = "s3.us-east-1.amazonaws.com"
          "spark.hadoop.fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        } : {}
      )
      custom_tags = {
        "ResourceClass" = "SingleNode"
        "Owner"         = "PATCHIT"
      }
    }
  }

  task {
    task_key        = "bronze_${each.key}"
    job_cluster_key = "patchit_job_cluster"
    notebook_task {
      notebook_path   = local.notebook_path_bronze
      source          = "WORKSPACE"
      base_parameters = each.value.base_notebook_params
    }
  }

  task {
    task_key        = "silver_${each.key}"
    job_cluster_key = "patchit_job_cluster"
    depends_on {
      task_key = "bronze_${each.key}"
    }
    notebook_task {
      notebook_path   = local.notebook_path_silver
      source          = "WORKSPACE"
      base_parameters = each.value.base_notebook_params
    }
  }

  task {
    task_key        = "gold_${each.key}"
    job_cluster_key = "patchit_job_cluster"
    depends_on {
      task_key = "silver_${each.key}"
    }
    notebook_task {
      notebook_path   = local.notebook_path_gold
      source          = "WORKSPACE"
      base_parameters = each.value.gold_notebook_params
    }
  }
}

resource "databricks_job" "retail_transactions_pipeline" {
  depends_on          = [databricks_schema.lakehouse]
  name                = "retail_transactions_pipeline"
  max_concurrent_runs = 1

  dynamic "run_as" {
    for_each = var.run_as_user != "" ? [1] : []
    content {
      user_name = var.run_as_user
    }
  }

  job_cluster {
    job_cluster_key = "patchit_job_cluster"
    new_cluster {
      spark_version      = var.job_spark_version
      node_type_id       = var.job_node_type_id
      num_workers        = 0
      data_security_mode = "USER_ISOLATION"
      azure_attributes {
        availability = "ON_DEMAND_AZURE"
      }
      spark_conf = merge(
        {
          "spark.databricks.cluster.profile" = "singleNode"
          "spark.master"                     = "local[*]"
        },
        trimspace(var.aws_secret_scope) != "" ? {
          "spark.hadoop.fs.s3.awsAccessKeyId"            = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_access_key_name}}}"
          "spark.hadoop.fs.s3.awsSecretAccessKey"        = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_secret_key_name}}}"
          "spark.hadoop.fs.s3n.awsAccessKeyId"           = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_access_key_name}}}"
          "spark.hadoop.fs.s3n.awsSecretAccessKey"       = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_secret_key_name}}}"
          "spark.hadoop.fs.s3a.access.key"               = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_access_key_name}}}"
          "spark.hadoop.fs.s3a.secret.key"               = "{{secrets/${trimspace(var.aws_secret_scope)}/${var.aws_secret_key_name}}}"
          "spark.hadoop.fs.s3a.endpoint"                 = "s3.us-east-1.amazonaws.com"
          "spark.hadoop.fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        } : {}
      )
      custom_tags = {
        "ResourceClass" = "SingleNode"
        "Owner"         = "PATCHIT"
      }
    }
  }

  task {
    task_key        = "retail_bronze_to_silver"
    job_cluster_key = "patchit_job_cluster"
    notebook_task {
      notebook_path = databricks_notebook.retail_silver_transform.path
      source        = "WORKSPACE"
      base_parameters = {
        catalog                = var.catalog
        schema                 = var.schema
        bucket                 = var.s3_bucket
        bronze_prefix          = "retail-live/bronze/transactions"
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
    }
  }

  task {
    task_key        = "retail_silver_to_gold"
    job_cluster_key = "patchit_job_cluster"
    depends_on {
      task_key = "retail_bronze_to_silver"
    }
    notebook_task {
      notebook_path = databricks_notebook.retail_gold_transform.path
      source        = "WORKSPACE"
      base_parameters = {
        catalog                = var.catalog
        schema                 = var.schema
        aws_secret_scope       = var.aws_secret_scope
        aws_access_key_name    = var.aws_access_key_name
        aws_secret_key_name    = var.aws_secret_key_name
        aws_session_token_name = var.aws_session_token_name
      }
    }
  }
}
