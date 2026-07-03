# ─────────────────────────────────────────────────────────────
# SHARED POLICIES — attached to whichever roles need them
# ─────────────────────────────────────────────────────────────

resource "aws_iam_policy" "s3_lake_access" {
  name = "${var.project}-s3-lake-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:GetBucketLocation",
      ]
      Resource = [
        var.lake_bucket_arn,
        "${var.lake_bucket_arn}/*",
      ]
    }]
  })
}

resource "aws_iam_policy" "s3_athena_results_access" {
  name = "${var.project}-s3-athena-results-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject", "s3:PutObject", "s3:GetBucketLocation", "s3:ListBucket",
      ]
      Resource = [
        var.athena_results_bucket_arn,
        "${var.athena_results_bucket_arn}/*",
      ]
    }]
  })
}

resource "aws_iam_policy" "glue_catalog_access" {
  name = "${var.project}-glue-catalog-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable",
        "glue:BatchCreatePartition", "glue:GetPartition",
        "glue:GetPartitions", "glue:BatchGetPartition",
      ]
      Resource = [
        "arn:aws:glue:${var.aws_region}:${var.account_id}:catalog",
        "arn:aws:glue:${var.aws_region}:${var.account_id}:database/${var.glue_database_name}",
        "arn:aws:glue:${var.aws_region}:${var.account_id}:table/${var.glue_database_name}/*",
      ]
    }]
  })
}

resource "aws_iam_policy" "dynamodb_ledger_access" {
  name = "${var.project}-dynamodb-ledger-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query", "dynamodb:UpdateItem",
      ]
      Resource = var.processed_files_table_arn
    }]
  })
}

resource "aws_iam_policy" "sns_publish_access" {
  name = "${var.project}-sns-publish-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sns:Publish"]
      Resource = var.notify_topic_arns
    }]
  })
}

# ─────────────────────────────────────────────────────────────
# ROLE: acquire Lambda
# Downloads zip, writes to raw/, checks/updates the DynamoDB ledger,
# publishes to its SNS topic.
# ─────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_acquire" {
  name = "${var.project}-lambda-acquire"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_acquire_logs" {
  role       = aws_iam_role.lambda_acquire.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_acquire_s3" {
  role       = aws_iam_role.lambda_acquire.name
  policy_arn = aws_iam_policy.s3_lake_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_acquire_ledger" {
  role       = aws_iam_role.lambda_acquire.name
  policy_arn = aws_iam_policy.dynamodb_ledger_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_acquire_sns" {
  role       = aws_iam_role.lambda_acquire.name
  policy_arn = aws_iam_policy.sns_publish_access.arn
}

# ─────────────────────────────────────────────────────────────
# ROLE: trigger_glue_zip Lambda
# Bridges S3 PUT event on raw/*.zip → starts the zip_to_bronze
# Glue job. Also handles EventBridge manual/cron triggers.
# ─────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_trigger_glue" {
  name = "${var.project}-lambda-trigger-glue"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_trigger_glue_logs" {
  role       = aws_iam_role.lambda_trigger_glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_trigger_glue_s3" {
  role       = aws_iam_role.lambda_trigger_glue.name
  policy_arn = aws_iam_policy.s3_lake_access.arn
}

resource "aws_iam_role_policy" "lambda_trigger_glue_start_job" {
  name = "${var.project}-lambda-trigger-glue-start-job"
  role = aws_iam_role.lambda_trigger_glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun"]
      Resource = "arn:aws:glue:${var.aws_region}:${var.account_id}:job/${var.project}-zip-to-bronze"
    }]
  })
}

# ─────────────────────────────────────────────────────────────
# ROLE: SES sender Lambda (subscribed to all 3 SNS topics)
# ─────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_ses_sender" {
  name = "${var.project}-lambda-ses-sender"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_ses_sender_logs" {
  role       = aws_iam_role.lambda_ses_sender.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_ses_sender_send_email" {
  name = "${var.project}-lambda-ses-sender-send-email"
  role = aws_iam_role.lambda_ses_sender.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ses:SendEmail", "ses:SendRawEmail"]
      Resource = "*"
    }]
  })
}

# ─────────────────────────────────────────────────────────────
# ROLE: Glue (shared by zip_to_bronze and bronze_to_silver)
# ─────────────────────────────────────────────────────────────

resource "aws_iam_role" "glue" {
  name = "${var.project}-glue"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.s3_lake_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_catalog" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_ledger" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.dynamodb_ledger_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_sns" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.sns_publish_access.arn
}


resource "aws_iam_policy" "glue_start_silver_job" {
  name = "${var.project}-glue-start-silver-job"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun"]
      Resource = "arn:aws:glue:${var.aws_region}:${var.account_id}:job/${var.project}-bronze-to-silver"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_start_silver_job" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_start_silver_job.arn
}

# ─────────────────────────────────────────────────────────────
# ROLE: Athena query execution context (used by the workgroup —
# Athena itself is serverless, but it needs S3 access on behalf
# of whoever runs queries; the workgroup result location uses
# this implicitly through the caller's own IAM, not a dedicated
# role. No additional role required here — kept as a comment for
# clarity in the debrief: Athena does not assume a role the way
# Lambda/Glue do, it acts as the calling principal.)
# ─────────────────────────────────────────────────────────────