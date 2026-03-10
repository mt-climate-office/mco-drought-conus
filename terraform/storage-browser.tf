# ============================================================
# STORAGE BROWSER — Amplify UI frontend for the mco-gridmet bucket
#
# Architecture:
#   Cognito Identity Pool (guest/unauthenticated) issues temporary AWS
#   credentials so the browser SDK can call s3:ListBucket / s3:GetObject.
#   A separate S3 bucket + CloudFront distribution serves the React app.
# ============================================================

# ---- Cognito Identity Pool (guest access, no login required) -----------------
resource "aws_cognito_identity_pool" "storage_browser" {
  identity_pool_name               = "${var.project_name}-storage-browser"
  allow_unauthenticated_identities = true
  allow_classic_flow               = false

  tags = local.common_tags
}

# ---- IAM: unauthenticated (guest) role with read-only S3 access --------------
resource "aws_iam_role" "cognito_unauthenticated" {
  name = "${var.project_name}-cognito-unauth"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "cognito-identity.amazonaws.com" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "cognito-identity.amazonaws.com:aud" = aws_cognito_identity_pool.storage_browser.id
        }
        "ForAnyValue:StringLike" = {
          "cognito-identity.amazonaws.com:amr" = "unauthenticated"
        }
      }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "cognito_unauthenticated_s3" {
  name = "s3-read-only"
  role = aws_iam_role.cognito_unauthenticated.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.outputs.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.outputs.arn
      }
    ]
  })
}

resource "aws_cognito_identity_pool_roles_attachment" "storage_browser" {
  identity_pool_id = aws_cognito_identity_pool.storage_browser.id
  roles = {
    unauthenticated = aws_iam_role.cognito_unauthenticated.arn
  }
}

# ---- S3 bucket: hosts the compiled React app --------------------------------
resource "aws_s3_bucket" "storage_browser_app" {
  bucket = "${var.s3_bucket_name}-browser-app"
  tags   = local.common_tags
}

resource "aws_s3_bucket_website_configuration" "storage_browser_app" {
  bucket = aws_s3_bucket.storage_browser_app.id
  index_document { suffix = "index.html" }
  error_document { key = "index.html" }
}

resource "aws_s3_bucket_public_access_block" "storage_browser_app" {
  bucket                  = aws_s3_bucket.storage_browser_app.id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "storage_browser_app_public" {
  bucket = aws_s3_bucket.storage_browser_app.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "PublicReadGetObject"
      Effect    = "Allow"
      Principal = "*"
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.storage_browser_app.arn}/*"
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.storage_browser_app]
}

# ---- CloudFront: serves the React app over HTTPS ----------------------------
resource "aws_cloudfront_distribution" "storage_browser" {
  origin {
    domain_name = aws_s3_bucket_website_configuration.storage_browser_app.website_endpoint
    origin_id   = "storage-browser-app"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  enabled             = true
  default_root_object = "index.html"
  comment             = "MCO Drought Storage Browser"

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "storage-browser-app"
    viewer_protocol_policy = "redirect-to-https"

    forwarded_values {
      query_string = false
      cookies { forward = "none" }
    }

    min_ttl     = 0
    default_ttl = 3600
    max_ttl     = 86400
  }

  # SPA routing: serve index.html for all paths
  custom_error_response {
    error_code         = 403
    response_code      = 200
    response_page_path = "/index.html"
  }
  custom_error_response {
    error_code         = 404
    response_code      = 200
    response_page_path = "/index.html"
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = local.common_tags
}
