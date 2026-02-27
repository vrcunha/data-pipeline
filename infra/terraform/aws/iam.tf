data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "ecs_task_execution_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${local.name_prefix}-ecs-task-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "bronze_task_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bronze_task" {
  name               = "${local.name_prefix}-bronze-task-role"
  assume_role_policy = data.aws_iam_policy_document.bronze_task_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "bronze_task_policy" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
    ]
  }

  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:CreateLogGroup",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "bronze_task" {
  name   = "${local.name_prefix}-bronze-task-policy"
  role   = aws_iam_role.bronze_task.id
  policy = data.aws_iam_policy_document.bronze_task_policy.json
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data_lake" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]

    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*",
      aws_s3_bucket.scripts.arn,
      "${aws_s3_bucket.scripts.arn}/*",
      aws_s3_bucket.athena_results.arn,
      "${aws_s3_bucket.athena_results.arn}/*",
    ]
  }

  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchUpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_data_lake" {
  name   = "${local.name_prefix}-glue-data-lake-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data_lake.json
}
