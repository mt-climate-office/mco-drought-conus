# EventBridge Scheduler fires at 6:00 PM America/Denver (Mountain Time) every day.
# The scheduler honors DST automatically: 6 PM MST (UTC-7) in winter,
# 6 PM MDT (UTC-6) in summer.
resource "aws_scheduler_schedule" "nightly" {
  name       = "${var.project_name}-nightly"
  group_name = "default"

  flexible_time_window {
    mode = "OFF" # start at exactly the scheduled time
  }

  schedule_expression          = var.schedule_time      # cron(0 18 * * ? *)
  schedule_expression_timezone = var.schedule_timezone  # America/Denver

  target {
    arn      = aws_ecs_cluster.drought.arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.pipeline.arn
      launch_type         = "FARGATE"
      task_count          = 1

      network_configuration {
        assign_public_ip = var.assign_public_ip
        subnets          = local.resolved_subnet_ids
        security_groups  = local.fargate_security_group_ids
      }
    }

    retry_policy {
      maximum_retry_attempts       = 0    # do not retry; next run is tonight
      maximum_event_age_in_seconds = 3600 # drop if not started within 1 h
    }
  }
}
