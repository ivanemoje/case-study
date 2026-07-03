# ─────────────────────────────────────────────────────────────
# DATA LAKE BUCKET
# Holds: raw/ (zips), bronze/, silver/, quarantine/, glue-scripts/,
#        lambda-packages/
# ─────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "lake" {
  bucket        = var.data_bucket_name
  force_destroy = true

  tags = { Project = var.project }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "prefixes" {
  for_each = toset([
    "raw/", "intermediate/", "bronze/", "silver/", "quarantine/",
    "glue-scripts/", "lambda-packages/"
  ])
  bucket  = aws_s3_bucket.lake.id
  key     = each.value
  content = ""
}

# ─────────────────────────────────────────────────────────────
# ATHENA RESULTS BUCKET — separate bucket, short lifecycle.
# Keeping this separate from the lake means:
#   - The lake's S3 event notifications never fire on query result
#     objects (would otherwise need exclusion filters everywhere)
#   - Lifecycle policy can be aggressive without touching real data
#   - Cost and access patterns are isolated and easy to reason about
# ─────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "athena_results" {
  bucket        = var.athena_results_bucket_name
  force_destroy = true

  tags = { Project = var.project }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {} # applies to all objects in the bucket

    expiration {
      days = var.athena_results_expiry_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
