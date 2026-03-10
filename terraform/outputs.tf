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

output "storage_browser_url" {
  description = "Public URL of the Storage Browser app"
  value       = "https://${aws_cloudfront_distribution.storage_browser.domain_name}"
}

output "storage_browser_identity_pool_id" {
  description = "Cognito Identity Pool ID used by the Storage Browser app"
  value       = aws_cognito_identity_pool.storage_browser.id
}

output "storage_browser_app_bucket" {
  description = "S3 bucket hosting the Storage Browser React app"
  value       = aws_s3_bucket.storage_browser_app.bucket
}

output "storage_browser_cloudfront_id" {
  description = "CloudFront distribution ID (needed for cache invalidation on deploy)"
  value       = aws_cloudfront_distribution.storage_browser.id
}
