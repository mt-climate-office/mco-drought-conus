resource "aws_ecr_repository" "pipeline" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

# Keep only the 5 most recent images to control storage costs
resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire images beyond the 5 most recent"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = { type = "expire" }
      }
    ]
  })
}
