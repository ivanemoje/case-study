terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  account_id          = data.aws_caller_identity.current.account_id
  lambdas_source_dir  = "${path.module}/../../../lambdas"
  glue_jobs_source_dir = "${path.module}/../../../glue_jobs"
}

# ─────────────────────────────────────────────────────────────
# S3 — data lake bucket, athena results bucket, processed-files ledger
# ─────────────────────────────────────────────────────────────

module "s3" {
  source = "../../modules/s3"

  project                     = var.project
  data_bucket_name            = var.data_bucket_name
  athena_results_bucket_name  = var.athena_results_bucket_name
  athena_results_expiry_days  = var.athena_results_expiry_days
}

# ─────────────────────────────────────────────────────────────
# SES — email identities + SNS topics for notifications
# ─────────────────────────────────────────────────────────────

module "ses" {
  source = "../../modules/ses"

  project             = var.project
  notification_email  = var.notification_email
  ses_from_email      = var.ses_from_email
}

# ─────────────────────────────────────────────────────────────
# IAM — all roles and policies
# ─────────────────────────────────────────────────────────────

module "iam" {
  source = "../../modules/iam"

  project                    = var.project
  aws_region                 = var.aws_region
  account_id                 = local.account_id
  athena_results_bucket_arn  = module.s3.athena_results_bucket_arn
  glue_database_name         = var.glue_database_name
  lake_bucket_arn            = module.s3.bucket_arn
  processed_files_table_arn  = module.s3.processed_files_table_arn
  notify_topic_arns = [
    module.ses.acquire_topic_arn,
    module.ses.landing_topic_arn,
    module.ses.silver_topic_arn,
  ]
}

# ─────────────────────────────────────────────────────────────
# GLUE — catalog database + two jobs + native job-chain trigger
# ─────────────────────────────────────────────────────────────

module "glue" {
  source = "../../modules/glue"

  project                      = var.project
  aws_region                   = var.aws_region
  lake_bucket_id               = module.s3.bucket_id
  glue_database_name           = var.glue_database_name
  glue_role_arn                = module.iam.glue_role_arn
  processed_files_table_name   = module.s3.processed_files_table_name
  landing_topic_arn            = module.ses.landing_topic_arn
  silver_topic_arn             = module.ses.silver_topic_arn
  glue_jobs_source_dir         = local.glue_jobs_source_dir
}

# ─────────────────────────────────────────────────────────────
# LAMBDA — acquire, trigger_glue_zip, ses_sender
# ─────────────────────────────────────────────────────────────

module "lambda" {
  source = "../../modules/lambda"

  project                       = var.project
  lake_bucket_id                = module.s3.bucket_id
  source_data_url               = var.source_data_url
  processed_files_table_name    = module.s3.processed_files_table_name
  acquire_topic_arn             = module.ses.acquire_topic_arn
  landing_topic_arn             = module.ses.landing_topic_arn
  silver_topic_arn              = module.ses.silver_topic_arn
  lambda_acquire_role_arn       = module.iam.lambda_acquire_role_arn
  lambda_trigger_glue_role_arn  = module.iam.lambda_trigger_glue_role_arn
  lambda_ses_sender_role_arn    = module.iam.lambda_ses_sender_role_arn
  zip_to_bronze_job_name        = module.glue.zip_to_bronze_job_name
  ses_from_email                = module.ses.ses_from_email
  notification_email            = module.ses.notification_email
  lambdas_source_dir            = local.lambdas_source_dir
}

# ─────────────────────────────────────────────────────────────
# EVENTBRIDGE — S3 PUT on raw/*.zip → trigger_glue_zip Lambda
# ─────────────────────────────────────────────────────────────

module "eventbridge" {
  source = "../../modules/eventbridge"

  project                          = var.project
  lake_bucket_id                   = module.s3.bucket_id
  trigger_glue_zip_function_name   = module.lambda.trigger_glue_zip_function_name
  trigger_glue_zip_function_arn    = module.lambda.trigger_glue_zip_function_arn
}

# ─────────────────────────────────────────────────────────────
# ATHENA — workgroup pointed at the separate results bucket
# ─────────────────────────────────────────────────────────────

module "athena" {
  source = "../../modules/athena"

  project                    = var.project
  athena_results_bucket_id   = module.s3.athena_results_bucket_id
}

