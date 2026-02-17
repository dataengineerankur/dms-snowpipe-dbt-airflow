output "storage_aws_external_id" {
  value       = snowflake_storage_integration.s3.storage_aws_external_id
  description = "External ID for AWS IAM role trust."
}

output "storage_aws_iam_user_arn" {
  value       = snowflake_storage_integration.s3.storage_aws_iam_user_arn
  description = "Snowflake AWS IAM user ARN for trust policy."
}

output "pipe_notification_channels" {
  value = {
    customers   = snowflake_pipe.customers.notification_channel
    products    = snowflake_pipe.products.notification_channel
    orders      = snowflake_pipe.orders.notification_channel
    order_items = snowflake_pipe.order_items.notification_channel
  }
  description = "SNS topic ARNs for Snowpipe auto-ingest."
}
