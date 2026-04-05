# PATCHIT auto-fix: unknown
# Original error: botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied
Bucket: dms-snowpipe-dev
Key: processed/customers/2026/04/05/data.parquet
Role: GlueServiceRole
locals {
  patchit_failure_glue_scripts = fileset("${path.module}/../../pipelines/aws_glue/jobs", "*.py")
  patchit_failure_glue_map = {
    for f in local.patchit_failure_glue_scripts :
    replace(f, ".py", "") => f
  }
}

resource "aws_s3_object" "patchit_failure_glue_script" {
  for_each = var.enable_patchit_failure_pack ? local.patchit_failure_glue_map : {}

  bucket = aws_s3_bucket.dms.bucket
  key    = "${var.patchit_failure_glue_prefix}/${each.value}"
  source = "${path.module}/../../pipelines/aws_glue/jobs/${each.value}"
  etag   = filemd5("${path.module}/../../pipelines/aws_glue/jobs/${each.value}")
}

resource "aws_glue_job" "patchit_failure_pack" {
  for_each = (var.enable_patchit_failure_pack && var.enable_glue_athena) ? local.patchit_failure_glue_map : {}

  name     = each.key
  role_arn = aws_iam_role.glue[0].arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.dms.bucket}/${var.patchit_failure_glue_prefix}/${each.value}"
  }

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = 2
  timeout           = 10

  default_arguments = {
    "--job-language"            = "python"
    "--enable-glue-datacatalog" = "true"
    "--TempDir"                 = "s3://${aws_s3_bucket.dms.bucket}/glue/tmp/"
    "--PATCHIT_FAILURE_PACK"    = "true"
    "--ISSUE_ID"                = upper(each.key)
  }

  depends_on = [aws_s3_object.patchit_failure_glue_script]
}
