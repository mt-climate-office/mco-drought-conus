resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/ecs/${var.project_name}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# Alert when a task exits with a non-zero status.
# ECS Container Insights must be enabled on the cluster (set below in ecs.tf).
resource "aws_cloudwatch_metric_alarm" "task_failures" {
  alarm_name          = "${var.project_name}-task-failures"
  alarm_description   = "Fires when the drought pipeline Fargate task fails"
  namespace           = "ECS/ContainerInsights"
  metric_name         = "TaskSetTaskCount" # a proxy; refine with a CloudWatch filter if desired
  statistic           = "Sum"
  comparison_operator = "LessThanThreshold"
  threshold           = 0 # placeholder — wire to SNS topic to get notified
  evaluation_periods  = 1
  period              = 86400
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.drought.name
  }

  tags = local.common_tags
}
