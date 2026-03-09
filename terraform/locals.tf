locals {
  common_tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
  }

  # Use explicitly provided subnet IDs, or fall back to all subnets in the VPC.
  resolved_subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.vpc.ids

  # Security groups for Fargate tasks and EFS mount targets.
  # No project-managed SG is created (NetworkDenyPolicy blocks ec2:CreateSecurityGroup).
  fargate_security_group_ids = var.extra_security_group_ids

  ecr_image_url = "${aws_ecr_repository.pipeline.repository_url}:latest"
}
