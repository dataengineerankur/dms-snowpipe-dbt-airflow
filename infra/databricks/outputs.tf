output "job_ids" {
  value = {
    for k, v in databricks_job.pipelines : k => v.id
  }
  description = "Databricks job ids for customers/products/orders workflows."
}
