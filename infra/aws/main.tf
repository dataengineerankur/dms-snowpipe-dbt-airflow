# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
# PATCHIT auto-fix: unknown
# Original error: (CloudWatch log fetch failed: An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log group does not exist.)
data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  count   = var.dms_vpc_id == null ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = length(var.dms_subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [local.dms_vpc_id]
  }
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  dms_vpc_id     = var.dms_vpc_id != null ? var.dms_vpc_id : data.aws_vpc.default[0].id
  dms_subnet_ids = length(var.dms_subnet_ids) > 0 ? var.dms_subnet_ids : data.aws_subnets.default[0].ids

  snowflake_principal_arn = var.snowflake_aws_iam_user_arn != "" ? var.snowflake_aws_iam_user_arn : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
  snowflake_external_id   = var.snowflake_external_id != "" ? var.snowflake_external_id : null

  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_name}-${var.environment}-${random_id.bucket_suffix.hex}"

  source_host = var.use_rds ? aws_db_instance.source[0].address : var.postgres_host

  redshift_enabled = var.enable_redshift || var.enable_redshift_serverless

  redshift_jdbc_url = local.redshift_enabled ? (var.enable_redshift_serverless ? "jdbc:redshift://${try(aws_redshiftserverless_workgroup.redshift[0].endpoint[0].address, "")}:5439/${var.redshift_database}" : "jdbc:redshift://${try(aws_redshift_cluster.redshift[0].endpoint, "")}:5439/${var.redshift_database}") : ""
}

resource "aws_s3_bucket" "dms" {
  bucket = local.bucket_name
  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "dms" {
  bucket                  = aws_s3_bucket.dms.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "dms" {
  bucket = aws_s3_bucket.dms.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dms" {
  bucket = aws_s3_bucket.dms.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_notification" "snowpipe" {
  count  = length(var.snowpipe_sqs_queue_arns) > 0 ? 1 : 0
  bucket = aws_s3_bucket.dms.id

  dynamic "queue" {
    for_each = var.snowpipe_sqs_queue_arns
    content {
      queue_arn     = queue.value
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = "dms/sales/${queue.key}/"
    }
  }
}

resource "aws_iam_role" "dms_access" {
  name = "${var.project_name}-${var.environment}-dms-s3-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "dms.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role" "dms_vpc_role" {
  name = "dms-vpc-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "dms.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dms_vpc_role" {
  role       = aws_iam_role.dms_vpc_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

resource "aws_iam_role_policy" "dms_access" {
  name = "${var.project_name}-${var.environment}-dms-s3-policy"
  role = aws_iam_role.dms_access.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:PutObjectTagging",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.dms.arn,
          "${aws_s3_bucket.dms.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "dms" {
  name              = "/aws/dms/${var.project_name}-${var.environment}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "dms_task" {
  name           = "dms-task"
  log_group_name = aws_cloudwatch_log_group.dms.name
}

resource "aws_security_group" "dms" {
  name   = "${var.project_name}-${var.environment}-dms-sg"
  vpc_id = local.dms_vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds" {
  count  = var.use_rds ? 1 : 0
  name   = "${var.project_name}-${var.environment}-rds-sg"
  vpc_id = local.dms_vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "rds_from_dms" {
  count                    = var.use_rds ? 1 : 0
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds[0].id
  source_security_group_id = aws_security_group.dms.id
}

resource "aws_security_group_rule" "rds_from_cidrs" {
  count             = var.use_rds && length(var.rds_allowed_cidrs) > 0 ? 1 : 0
  type              = "ingress"
  from_port         = 5432
  to_port           = 5432
  protocol          = "tcp"
  security_group_id = aws_security_group.rds[0].id
  cidr_blocks       = var.rds_allowed_cidrs
}

resource "aws_dms_replication_subnet_group" "dms" {
  replication_subnet_group_description = "DMS subnet group"
  replication_subnet_group_id          = "${var.project_name}-${var.environment}-dms-subnets"
  subnet_ids                           = local.dms_subnet_ids
}

resource "aws_dms_replication_instance" "dms" {
  replication_instance_id     = "${var.project_name}-${var.environment}-dms"
  replication_instance_class  = var.dms_replication_instance_class
  allocated_storage           = var.dms_allocated_storage_gb
  vpc_security_group_ids      = [aws_security_group.dms.id]
  replication_subnet_group_id = aws_dms_replication_subnet_group.dms.id
  multi_az                    = false
  publicly_accessible         = true
}

resource "aws_dms_endpoint" "source" {
  endpoint_id   = "${var.project_name}-${var.environment}-pg-source"
  endpoint_type = "source"
  engine_name   = "postgres"

  server_name   = local.source_host
  port          = var.postgres_port
  database_name = var.postgres_db
  username      = var.postgres_user
  password      = var.postgres_password
  ssl_mode      = "require"
}

resource "aws_dms_endpoint" "target" {
  endpoint_id   = "${var.project_name}-${var.environment}-s3-target"
  endpoint_type = "target"
  engine_name   = "s3"

  s3_settings {
    bucket_name               = aws_s3_bucket.dms.bucket
    bucket_folder             = "dms"
    compression_type          = "GZIP"
    data_format               = "parquet"
    parquet_version           = "parquet-1-0"
    service_access_role_arn   = aws_iam_role.dms_access.arn
    date_partition_enabled    = true
    date_partition_sequence   = "YYYYMMDD"
    date_partition_delimiter  = "SLASH"
    include_op_for_full_load  = true
    timestamp_column_name     = "dms_commit_ts"
  }
}

resource "aws_dms_replication_task" "full_load_cdc" {
  replication_task_id       = "${var.project_name}-${var.environment}-full-cdc"
  replication_instance_arn  = aws_dms_replication_instance.dms.replication_instance_arn
  source_endpoint_arn       = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn       = aws_dms_endpoint.target.endpoint_arn
  migration_type            = "full-load-and-cdc"
  table_mappings            = file("${path.module}/table_mappings.json")
  replication_task_settings = file("${path.module}/dms_task_settings.json")
  start_replication_task    = false
}

resource "aws_db_subnet_group" "rds" {
  count       = var.use_rds ? 1 : 0
  name        = "${var.project_name}-${var.environment}-rds-subnets"
  description = "RDS subnet group"
  subnet_ids  = local.dms_subnet_ids
}

resource "aws_db_parameter_group" "rds" {
  count  = var.use_rds ? 1 : 0
  name   = "${var.project_name}-${var.environment}-rds-params"
  family = var.rds_parameter_group_family

  parameter {
    apply_method = "pending-reboot"
    name  = "rds.logical_replication"
    value = "1"
  }

  parameter {
    apply_method = "pending-reboot"
    name  = "max_replication_slots"
    value = "5"
  }

  parameter {
    apply_method = "pending-reboot"
    name  = "max_wal_senders"
    value = "5"
  }
}

resource "aws_db_instance" "source" {
