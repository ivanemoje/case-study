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
  account_id           = data.aws_caller_identity.current.account_id
  lambdas_source_dir   = "${path.module}/../../../lambdas"
  glue_jobs_source_dir = "${path.module}/../../../glue_jobs"
}

module "s3" {
  source = "../../modules/s3"

  project                    = var.project
  data_bucket_name           = var.data_bucket_name
  athena_results_bucket_name = var.athena_results_bucket_name
  athena_results_expiry_days = var.athena_results_expiry_days
}

module "notifications" {
  source = "../../modules/notifications"

  project            = var.project
  notification_email = var.notification_email
}

module "iam" {
  source = "../../modules/iam"

  project                   = var.project
  aws_region                = var.aws_region
  account_id                = local.account_id
  glue_database_name        = var.glue_database_name
  lake_bucket_arn           = module.s3.bucket_arn
  processed_files_table_arn = module.s3.processed_files_table_arn
  notification_topic_arn    = module.notifications.topic_arn
}

module "glue" {
  source = "../../modules/glue"

  project                    = var.project
  aws_region                 = var.aws_region
  lake_bucket_id             = module.s3.bucket_id
  glue_database_name         = var.glue_database_name
  glue_role_arn              = module.iam.glue_role_arn
  processed_files_table_name = module.s3.processed_files_table_name
  notification_topic_arn     = module.notifications.topic_arn
  glue_jobs_source_dir       = local.glue_jobs_source_dir
}

module "lambda" {
  source = "../../modules/lambda"

  project                      = var.project
  lake_bucket_id               = module.s3.bucket_id
  source_data_url              = var.source_data_url
  processed_files_table_name   = module.s3.processed_files_table_name
  notification_topic_arn       = module.notifications.topic_arn
  lambda_acquire_role_arn      = module.iam.lambda_acquire_role_arn
  lambda_trigger_glue_role_arn = module.iam.lambda_trigger_glue_role_arn
  zip_to_bronze_job_name       = module.glue.zip_to_bronze_job_name
  lambdas_source_dir           = local.lambdas_source_dir
}

module "eventbridge" {
  source = "../../modules/eventbridge"

  project                        = var.project
  lake_bucket_id                 = module.s3.bucket_id
  trigger_glue_zip_function_name = module.lambda.trigger_glue_zip_function_name
  trigger_glue_zip_function_arn  = module.lambda.trigger_glue_zip_function_arn
}

module "athena" {
  source = "../../modules/athena"

  project                  = var.project
  athena_results_bucket_id = module.s3.athena_results_bucket_id
}
