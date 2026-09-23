resource "aws_s3_bucket" "outputs" {
  bucket = var.s3_bucket_name
  tags   = local.common_tags
}

# Allow public reads (block public access settings must be disabled first)
resource "aws_s3_bucket_public_access_block" "outputs" {
  bucket = aws_s3_bucket.outputs.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "outputs_public_read" {
  bucket = aws_s3_bucket.outputs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.outputs.arn}/*"
      },
      {
        # Public ListBucket lets anonymous requests (including the CDN's
        # pass-through origin) distinguish missing keys: 404 instead of 403.
        # Codified 2026-08-08 from a console-made change that Terraform
        # would otherwise silently revert.
        Sid       = "PublicListBucket"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:ListBucket"
        Resource  = aws_s3_bucket.outputs.arn
      }
    ]
  })

  # public_access_block must be applied before the public bucket policy
  depends_on = [aws_s3_bucket_public_access_block.outputs]
}

resource "aws_s3_bucket_cors_configuration" "outputs" {
  bucket = aws_s3_bucket.outputs.id

  # Origins are restricted to the data CDN: browser consumers go through
  # data2.climate.umt.edu, not the raw S3 endpoint. Codified 2026-08-08 from
  # a console-made tightening that Terraform would otherwise loosen back
  # to allowed_origins = ["*"].
  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET"]
    allowed_origins = [
      "https://d1s8jav5n0eyyf.cloudfront.net",
      "https://data2.climate.umt.edu",
    ]
    max_age_seconds = 3600
  }
}
