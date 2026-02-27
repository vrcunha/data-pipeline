output "s3_buckets" {
  description = "S3 buckets that replace MinIO buckets in AWS."
  value = {
    bronze         = aws_s3_bucket.bronze.id
    silver         = aws_s3_bucket.silver.id
    gold           = aws_s3_bucket.gold.id
    scripts        = aws_s3_bucket.scripts.id
    athena_results = aws_s3_bucket.athena_results.id
  }
}

output "ecs" {
  description = "ECS resources for bronze execution via RunTask."
  value = {
    cluster_arn          = aws_ecs_cluster.this.arn
    cluster_name         = aws_ecs_cluster.this.name
    task_definition_arn  = aws_ecs_task_definition.bronze.arn
    security_group_id    = aws_security_group.ecs_tasks.id
    subnet_ids           = [for s in aws_subnet.public : s.id]
  }
}

output "glue_jobs" {
  description = "Glue jobs for silver and gold Spark processing."
  value = {
    silver_job_name = aws_glue_job.silver.name
    gold_job_name   = aws_glue_job.gold.name
  }
}

output "glue_catalog_database" {
  description = "Glue Catalog database used by Athena."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "athena_workgroup" {
  description = "Athena workgroup for querying lakehouse data."
  value       = aws_athena_workgroup.lakehouse.name
}
