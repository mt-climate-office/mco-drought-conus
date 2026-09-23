# ── GitHub Actions OIDC ───────────────────────────────────────────────────────
# Deploy role for THIS repo's CI. Until 2026-08 this repo's workflow assumed
# mco-snodas's github-actions-ecr-push role via a console-widened trust policy
# that mco-snodas's terraform would silently revert on its next apply. Each
# repo now owns its role: ECR push only, scoped to this repo's registry.

data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_role" "github_actions" {
  name                 = "${var.project_name}-github-actions"
  max_session_duration = 3600 # image pushes take minutes, not hours

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRoleWithWebIdentity"
      Principal = {
        Federated = data.aws_iam_openid_connect_provider.github.arn
      }
      Condition = {
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:mt-climate-office/mco-drought-conus:*"
        }
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "github_actions_ecr_push" {
  name = "ecr-push"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRPush"
        Effect = "Allow"
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability",
          "ecr:PutImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
        ]
        Resource = aws_ecr_repository.pipeline.arn
      },
      {
        Sid      = "ECRAuth"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*" # the API requires *; the token only unlocks repos the role can otherwise reach
      },
    ]
  })
}

output "github_actions_role_arn" {
  description = "IAM role ARN assumed by GitHub Actions for ECR push"
  value       = aws_iam_role.github_actions.arn
}
