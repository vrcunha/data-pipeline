# AWS Terraform - DE Case Lakehouse

This Terraform stack provisions an AWS equivalent for your current Docker Compose architecture.

## What it creates

- S3 buckets (replacing MinIO):
  - bronze
  - silver
  - gold
  - scripts (job artifacts)
  - athena-results
- ECS (Fargate) for Bronze execution:
  - ECS Cluster
  - Bronze Task Definition
  - IAM roles/policies
  - Log Group
  - Minimal VPC + public subnets + security group
- AWS Glue jobs for Silver and Gold (Spark):
  - Glue Job `silver`
  - Glue Job `gold`
  - Glue crawlers for Delta paths (silver and gold)
- Glue Catalog:
  - Database `lakehouse`
- Athena:
  - Workgroup for querying cataloged tables

## Prerequisites

- Terraform >= 1.5
- AWS credentials configured in your shell/profile
- Bronze Docker image pushed to ECR

## Usage

```bash
cd infra/terraform/aws
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

## Airflow integration hints

- Bronze: run ECS task (RunTask) using outputs:
  - `ecs.cluster_name`
  - `ecs.task_definition_arn`
  - `ecs.subnet_ids`
  - `ecs.security_group_id`
- Silver/Gold: run Glue jobs using outputs:
  - `glue_jobs.silver_job_name`
  - `glue_jobs.gold_job_name`
- Query layer via Athena workgroup + Glue Catalog:
  - `athena_workgroup`
  - `glue_catalog_database`

## Notes

- Job code is uploaded to S3 from local paths:
  - `jobs/main.py`
  - zipped `data_pipeline/` as `libs/data_pipeline.zip`
- Default Glue arguments are placeholders and should be overridden by orchestration (Airflow) per run, mainly:
  - `--execution_date`
  - `--context`
