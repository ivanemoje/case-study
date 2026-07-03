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
  function_name = "${var.project}-acquire"
  role          = var.lambda_acquire_role_arn
  runtime       = "python3.12"
  handler       = "handler.lambda_handler"
  timeout       = 900
  memory_size   = 512

  ephemeral_storage {
    size = 1024
  }

  s3_bucket        = var.lake_bucket_id
  s3_key           = aws_s3_object.lambda_acquire_pkg.key
  source_code_hash = data.archive_file.lambda_acquire.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME   = var.lake_bucket_id
      SOURCE_URL    = var.source_data_url
      LEDGER_TABLE  = var.processed_files_table_name
      SNS_TOPIC_ARN = var.notification_topic_arn
    }
  }

  tags = { Project = var.project }
}

resource "aws_cloudwatch_log_group" "lambda_acquire" {
  name              = "/aws/lambda/${aws_lambda_function.acquire.function_name}"
  retention_in_days = 7
}

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
  function_name = "${var.project}-trigger-glue-zip"
  role          = var.lambda_trigger_glue_role_arn
  runtime       = "python3.12"
  handler       = "handler.lambda_handler"
  timeout       = 30
  memory_size   = 128

  s3_bucket        = var.lake_bucket_id
  s3_key           = aws_s3_object.lambda_trigger_glue_pkg.key
  source_code_hash = data.archive_file.lambda_trigger_glue.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME  = var.zip_to_bronze_job_name
      BUCKET_NAME    = var.lake_bucket_id
      LANDING_PREFIX = "landing/"
    }
  }

  tags = { Project = var.project }
}

resource "aws_cloudwatch_log_group" "lambda_trigger_glue" {
  name              = "/aws/lambda/${aws_lambda_function.trigger_glue_zip.function_name}"
  retention_in_days = 7
}
