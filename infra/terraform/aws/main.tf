locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = merge(var.tags, {
    environment = var.environment
    project     = var.project_name
  })
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "aws_s3_bucket" "bronze" {
  bucket        = "${local.name_prefix}-bronze-${random_string.suffix.result}"
  force_destroy = var.force_destroy_buckets
  tags          = local.common_tags
}

resource "aws_s3_bucket" "silver" {
  bucket        = "${local.name_prefix}-silver-${random_string.suffix.result}"
  force_destroy = var.force_destroy_buckets
  tags          = local.common_tags
}

resource "aws_s3_bucket" "gold" {
  bucket        = "${local.name_prefix}-gold-${random_string.suffix.result}"
  force_destroy = var.force_destroy_buckets
  tags          = local.common_tags
}

resource "aws_s3_bucket" "scripts" {
  bucket        = "${local.name_prefix}-scripts-${random_string.suffix.result}"
  force_destroy = var.force_destroy_buckets
  tags          = local.common_tags
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.name_prefix}-athena-results-${random_string.suffix.result}"
  force_destroy = var.force_destroy_buckets
  tags          = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    bronze         = aws_s3_bucket.bronze.id
    silver         = aws_s3_bucket.silver.id
    gold           = aws_s3_bucket.gold.id
    scripts        = aws_s3_bucket.scripts.id
    athena_results = aws_s3_bucket.athena_results.id
  }

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "archive_file" "data_pipeline_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../../data_pipeline"
  output_path = "${path.module}/data_pipeline.zip"
}

resource "aws_s3_object" "job_main" {
  bucket = aws_s3_bucket.scripts.id
  key    = "jobs/main.py"
  source = "${path.module}/../../../jobs/main.py"
  etag   = filemd5("${path.module}/../../../jobs/main.py")
}

resource "aws_s3_object" "data_pipeline_lib" {
  bucket = aws_s3_bucket.scripts.id
  key    = "libs/data_pipeline.zip"
  source = data.archive_file.data_pipeline_zip.output_path
  etag   = data.archive_file.data_pipeline_zip.output_md5
}
