resource "aws_ecs_cluster" "this" {
  name = "${local.name_prefix}-cluster"
  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "ecs_bronze" {
  name              = "/ecs/${local.name_prefix}/bronze"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_ecs_task_definition" "bronze" {
  family                   = "${local.name_prefix}-bronze"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.ecs_bronze_task_cpu)
  memory                   = tostring(var.ecs_bronze_task_memory)
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.bronze_task.arn

  container_definitions = jsonencode([
    {
      name      = "bronze"
      image     = var.bronze_image_uri
      essential = true
      command   = ["bronze.py"]
      environment = [
        {
          name  = "AWS_S3_REGION"
          value = var.aws_region
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_bronze.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  tags = local.common_tags
}
