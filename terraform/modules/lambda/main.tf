# ─────────────────────────────────────────────────────────────
# LAMBDA: acquire
# Downloads the zip from SOURCE_URL, computes its SHA256 checksum,
# checks the DynamoDB ledger — if already processed, skips and
# notifies via SNS without re-uploading. Otherwise uploads the zip
# AS-IS (still compressed) to raw/, records the checksum in the
# ledger, and publishes a result to SNS.
#
# It does NOT extract the CSV. That happens in the zip_to_bronze
# Glue job, which has Spark's memory management instead of Lambda's
# fixed memory ceiling — appropriate for "big data" CSVs that may
# not comfortably fit in Lambda's /tmp or memory limits.
# ─────────────────────────────────────────────────────────────

data "archive_file" "lambda_acquire" {
  type        = "zip"
  source_file = "${var.lambdas_source_dir}/acquire/handler.py"
  output_path = "${var.lambdas_source_dir}/acquire/package.zip"
}

resource "aws_s3_object" "lambda_acquire_pkg" {
  bucket = var.lake_bucket_id
  key    = "lambda-packages/acquire.zip"
  source = data.archive_file.lambda_acquire.output_path
  etag   = data.archive_file.lambda_acquire.output_md5
}

resource "aws_lambda_function" "acquire" {
  function_name    = "${var.project}-acquire"
  role             = var.lambda_acquire_role_arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  timeout          = 900
  memory_size      = 1024

  s3_bucket        = var.lake_bucket_id
  s3_key           = aws_s3_object.lambda_acquire_pkg.key
  source_code_hash = data.archive_file.lambda_acquire.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME    = var.lake_bucket_id
      SOURCE_URL     = var.source_data_url
      LEDGER_TABLE   = var.processed_files_table_name
      SNS_TOPIC_ARN  = var.acquire_topic_arn
    }
  }

  tags = { Project = var.project }
}

resource "aws_cloudwatch_log_group" "lambda_acquire" {
  name              = "/aws/lambda/${aws_lambda_function.acquire.function_name}"
  retention_in_days = 7
}

# ─────────────────────────────────────────────────────────────
# LAMBDA: trigger_glue_zip
# Bridges S3 PUT events on raw/*.zip → starts the zip_to_bronze
# Glue job, passing the S3 key as a job argument. Also handles
# manual invocation (drop a file via console or CLI) and could be
# wired to a cron via EventBridge the same way.
# ─────────────────────────────────────────────────────────────

data "archive_file" "lambda_trigger_glue" {
  type        = "zip"
  source_file = "${var.lambdas_source_dir}/trigger_glue_zip/handler.py"
  output_path = "${var.lambdas_source_dir}/trigger_glue_zip/package.zip"
}

resource "aws_s3_object" "lambda_trigger_glue_pkg" {
  bucket = var.lake_bucket_id
  key    = "lambda-packages/trigger_glue_zip.zip"
  source = data.archive_file.lambda_trigger_glue.output_path
  etag   = data.archive_file.lambda_trigger_glue.output_md5
}

resource "aws_lambda_function" "trigger_glue_zip" {
  function_name    = "${var.project}-trigger-glue-zip"
  role             = var.lambda_trigger_glue_role_arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  timeout          = 30
  memory_size      = 128

  s3_bucket        = var.lake_bucket_id
  s3_key           = aws_s3_object.lambda_trigger_glue_pkg.key
  source_code_hash = data.archive_file.lambda_trigger_glue.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME = var.zip_to_bronze_job_name
      BUCKET_NAME   = var.lake_bucket_id
      RAW_PREFIX    = "raw/"
    }
  }

  tags = { Project = var.project }
}

resource "aws_cloudwatch_log_group" "lambda_trigger_glue" {
  name              = "/aws/lambda/${aws_lambda_function.trigger_glue_zip.function_name}"
  retention_in_days = 7
}

# ─────────────────────────────────────────────────────────────
# LAMBDA: ses_sender
# Subscribed to all three SNS topics (acquire, landing, silver).
# Single place that knows how to format and send the actual email
# via SES — keeps the email template out of Glue jobs and out of
# the acquire Lambda.
# ─────────────────────────────────────────────────────────────

data "archive_file" "lambda_ses_sender" {
  type        = "zip"
  source_file = "${var.lambdas_source_dir}/ses_sender/handler.py"
  output_path = "${var.lambdas_source_dir}/ses_sender/package.zip"
}

resource "aws_s3_object" "lambda_ses_sender_pkg" {
  bucket = var.lake_bucket_id
  key    = "lambda-packages/ses_sender.zip"
  source = data.archive_file.lambda_ses_sender.output_path
  etag   = data.archive_file.lambda_ses_sender.output_md5
}

resource "aws_lambda_function" "ses_sender" {
  function_name    = "${var.project}-ses-sender"
  role             = var.lambda_ses_sender_role_arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  timeout          = 15
  memory_size      = 128

  s3_bucket        = var.lake_bucket_id
  s3_key           = aws_s3_object.lambda_ses_sender_pkg.key
  source_code_hash = data.archive_file.lambda_ses_sender.output_base64sha256

  environment {
    variables = {
      SES_FROM_EMAIL      = var.ses_from_email
      NOTIFICATION_EMAIL  = var.notification_email
    }
  }

  tags = { Project = var.project }
}

resource "aws_cloudwatch_log_group" "lambda_ses_sender" {
  name              = "/aws/lambda/${aws_lambda_function.ses_sender.function_name}"
  retention_in_days = 7
}

# Subscribe ses_sender to all three SNS topics

resource "aws_sns_topic_subscription" "ses_sender_acquire" {
  topic_arn = var.acquire_topic_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.ses_sender.arn
}

resource "aws_sns_topic_subscription" "ses_sender_landing" {
  topic_arn = var.landing_topic_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.ses_sender.arn
}

resource "aws_sns_topic_subscription" "ses_sender_silver" {
  topic_arn = var.silver_topic_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.ses_sender.arn
}

resource "aws_lambda_permission" "allow_sns_acquire" {
  statement_id  = "AllowSNSAcquire"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ses_sender.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = var.acquire_topic_arn
}

resource "aws_lambda_permission" "allow_sns_landing" {
  statement_id  = "AllowSNSLanding"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ses_sender.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = var.landing_topic_arn
}

resource "aws_lambda_permission" "allow_sns_silver" {
  statement_id  = "AllowSNSSilver"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ses_sender.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = var.silver_topic_arn
}
