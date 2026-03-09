output "s3_bucket_name" {
  description = "Public S3 bucket for drought indicator outputs"
  value       = aws_s3_bucket.outputs.bucket
}

output "s3_bucket_url" {
  description = "HTTPS URL of the public S3 bucket"
  value       = "https://${aws_s3_bucket.outputs.bucket_regional_domain_name}"
}

output "ecr_repository_url" {
  description = "ECR repository URL — use this when tagging/pushing the Docker image"
  value       = aws_ecr_repository.pipeline.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.drought.name
}

output "ecs_task_definition_arn" {
  description = "Latest ECS task definition ARN"
  value       = aws_ecs_task_definition.pipeline.arn
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group for pipeline runs"
  value       = aws_cloudwatch_log_group.pipeline.name
}

output "scheduler_arn" {
  description = "EventBridge Scheduler ARN (nightly trigger)"
  value       = aws_scheduler_schedule.nightly.arn
}
