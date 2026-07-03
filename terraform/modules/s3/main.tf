resource "aws_s3_bucket" "lake" {
  bucket        = var.data_bucket_name
  force_destroy = true
  tags          = { Project = var.project }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
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
    "landing/",
    "archive/",
    "staging/",
    "bronze/",
    "silver/",
    "quarantine/",
    "glue-scripts/",
    "lambda-packages/",
  ])

  bucket  = aws_s3_bucket.lake.id
  key     = each.value
  content = ""
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_versioning.lake]

  rule {
    id     = "expire-staging"
    status = "Enabled"
    filter { prefix = "staging/" }

    expiration {
      days = 1
    }
  }

  rule {
    id     = "expire-stale-landing"
    status = "Enabled"
    filter { prefix = "landing/" }

    expiration {
      days = 30
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = var.athena_results_bucket_name
  force_destroy = true
  tags          = { Project = var.project }
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
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}

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
