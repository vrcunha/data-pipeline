variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project slug used in resource names."
  type        = string
  default     = "data-pipeline"
}

variable "environment" {
  description = "Environment name (e.g. dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "table_name" {
  description = "Logical table/source name used by jobs."
  type        = string
  default     = "breweries"
}

variable "bronze_image_uri" {
  description = "ECR image URI for the bronze container (e.g. <account>.dkr.ecr.<region>.amazonaws.com/bronze:tag)."
  type        = string
}

variable "force_destroy_buckets" {
  description = "Allow bucket deletion even when non-empty (recommended only for non-prod)."
  type        = bool
  default     = true
}

variable "ecs_bronze_task_cpu" {
  description = "CPU units for ECS bronze task (Fargate)."
  type        = number
  default     = 512
}

variable "ecs_bronze_task_memory" {
  description = "Memory (MiB) for ECS bronze task (Fargate)."
  type        = number
  default     = 1024
}

variable "glue_version" {
  description = "AWS Glue version for silver and gold jobs."
  type        = string
  default     = "5.0"
}

variable "glue_worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers for each Glue job."
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Max retries for Glue jobs."
  type        = number
  default     = 1
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name."
  type        = string
  default     = "lakehouse"
}

variable "tags" {
  description = "Common tags applied to resources."
  type        = map(string)
  default = {
    managed_by = "terraform"
    project    = "data-pipeline"
  }
}
