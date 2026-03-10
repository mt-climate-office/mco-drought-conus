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
      }
    ]
  })

  # public_access_block must be applied before the public bucket policy
  depends_on = [aws_s3_bucket_public_access_block.outputs]
}

resource "aws_s3_bucket_cors_configuration" "outputs" {
  bucket = aws_s3_bucket.outputs.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["Content-Range", "Accept-Ranges", "Content-Length", "ETag"]
    max_age_seconds = 3600
  }
}
