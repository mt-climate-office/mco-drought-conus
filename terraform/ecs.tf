resource "aws_ecs_cluster" "drought" {
  name = var.project_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.common_tags
}

resource "aws_ecs_cluster_capacity_providers" "drought" {
  cluster_name       = aws_ecs_cluster.drought.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 100
    base              = 1
  }
}

resource "aws_ecs_task_definition" "pipeline" {
  family                   = var.project_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  # GridMET cache + interim data live here during the run.
  # On cold start the full historical record is downloaded from GridMET
  # (~15–20 GB); subsequent runs restore the cache from S3 in minutes.
  ephemeral_storage {
    size_in_gib = var.fargate_ephemeral_storage_gib
  }

  container_definitions = jsonencode([
    {
      name      = "drought-pipeline"
      image     = local.ecr_image_url
      essential = true

      environment = [
        { name = "TZ",                    value = "America/Denver" },
        { name = "DATA_DIR",              value = "/data" },
        { name = "PROJECT_DIR",           value = "/opt/app" },
        { name = "CORES",                 value = tostring(var.container_cores) },
        { name = "KEEP_TILES",            value = "0" },
        { name = "CONUS_MASK",            value = "1" },
        { name = "TILE_DX",               value = "2" },
        { name = "TILE_DY",               value = "2" },
        { name = "GRIDMET_REFRESH_YEARS", value = tostring(var.gridmet_refresh_years) },
        { name = "START_YEAR",            value = tostring(var.start_year) },
        { name = "CLIM_PERIODS",          value = var.clim_periods },
        { name = "AWS_BUCKET",            value = var.s3_bucket_name },
        { name = "AWS_DEFAULT_REGION",    value = var.aws_region },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.pipeline.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "pipeline"
        }
      }

      ulimits = [
        {
          name      = "nofile"
          softLimit = 65536
          hardLimit = 65536
        }
      ]
    }
  ])

  tags = local.common_tags
}
