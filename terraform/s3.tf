locals {
  bucket_layers = ["bronze", "silver", "gold", "scripts", "athena-dbt"]
}

resource "aws_s3_bucket" "layers" {
  for_each = toset(local.bucket_layers)

  bucket        = "${local.prefix}-${each.key}" # bucket name
  force_destroy = true
  # Insert some metadata
  tags = {
    Layer = title(each.key) # capitalizes the layer name (Bronze, Silver, etc.)
  }
}

resource "aws_s3_bucket_versioning" "layers" {
  for_each = aws_s3_bucket.layers

  bucket = each.value.id # point to previous bucket
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layers" {
  for_each = aws_s3_bucket.layers

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Ensure buckets cannot be accidentally made public
resource "aws_s3_bucket_public_access_block" "layers" {
  for_each = aws_s3_bucket.layers

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "athena_dbt_bucket_name" {
  value = aws_s3_bucket.layers["athena-dbt"].id
}