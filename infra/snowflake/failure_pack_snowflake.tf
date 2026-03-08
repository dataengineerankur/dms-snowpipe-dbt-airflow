locals {
  patchit_failure_sql_files = fileset("${path.module}/../../pipelines/snowflake/sql", "*.sql")
}

resource "null_resource" "patchit_failure_sql" {
  for_each = (var.enable_patchit_failure_pack && var.snowsql_connection_name != "") ? toset(local.patchit_failure_sql_files) : []

  triggers = {
    file_hash = filemd5("${path.module}/../../pipelines/snowflake/sql/${each.value}")
  }

  provisioner "local-exec" {
    command = "snowsql -c ${var.snowsql_connection_name} -f ${path.module}/../../pipelines/snowflake/sql/${each.value}"
  }
}
