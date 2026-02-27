aws_region      = "us-east-1"
project_name    = "data-pipeline"
environment     = "dev"
table_name      = "breweries"

# Build/push your bronze image to ECR and provide full URI.
bronze_image_uri = "339712752561.dkr.ecr.us-east-1.amazonaws.com/data-pipeline-bronze:latest"

force_destroy_buckets = true

ecs_bronze_task_cpu    = 512
ecs_bronze_task_memory = 1024

glue_version           = "5.0"
glue_worker_type       = "G.1X"
glue_number_of_workers = 2
glue_max_retries       = 1

athena_workgroup_name = "lakehouse"
