locals {
  silver_context = jsonencode({
    source_bucket      = aws_s3_bucket.bronze.id
    source_path        = "${var.table_name}/"
    destination_bucket = aws_s3_bucket.silver.id
    destination_path   = "${var.table_name}/"
  })

  gold_context = jsonencode({
    source_bucket      = aws_s3_bucket.silver.id
    source_path        = "${var.table_name}/"
    destination_bucket = aws_s3_bucket.gold.id
    destination_path   = "${var.table_name}/"
  })
}

resource "aws_glue_catalog_database" "lakehouse" {
  name = "lakehouse"
}

resource "aws_glue_job" "silver" {
  name              = "${local.name_prefix}-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.job_main.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                   = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"       = "true"
    "--datalake-formats"              = "delta"
    "--extra-py-files"                = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.data_pipeline_lib.key}"
    "--layer"                         = "silver"
    "--source"                        = var.table_name
    "--execution_date"                = "1970-01-01"
    "--context"                       = local.silver_context
  }

  tags = local.common_tags
}

resource "aws_glue_job" "gold" {
  name              = "${local.name_prefix}-gold"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  max_retries       = var.glue_max_retries
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.job_main.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                   = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"       = "true"
    "--datalake-formats"              = "delta"
    "--extra-py-files"                = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.data_pipeline_lib.key}"
    "--layer"                         = "gold"
    "--source"                        = var.table_name
    "--execution_date"                = "1970-01-01"
    "--context"                       = local.gold_context
  }

  tags = local.common_tags
}

resource "aws_glue_crawler" "silver_delta" {
  name          = "${local.name_prefix}-silver-delta-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.silver.id}/${var.table_name}/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags
}

resource "aws_glue_crawler" "gold_delta" {
  name          = "${local.name_prefix}-gold-delta-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    delta_tables              = ["s3://${aws_s3_bucket.gold.id}/${var.table_name}/"]
    write_manifest            = false
    create_native_delta_table = true
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = local.common_tags
}
