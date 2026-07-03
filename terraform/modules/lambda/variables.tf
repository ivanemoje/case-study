variable "project" {
  type = string
}

variable "lake_bucket_id" {
  type = string
}

variable "source_data_url" {
  type = string
}

variable "processed_files_table_name" {
  type = string
}

variable "acquire_topic_arn" {
  type = string
}

variable "landing_topic_arn" {
  type = string
}

variable "silver_topic_arn" {
  type = string
}

variable "lambda_acquire_role_arn" {
  type = string
}

variable "lambda_trigger_glue_role_arn" {
  type = string
}

variable "lambda_ses_sender_role_arn" {
  type = string
}

variable "zip_to_bronze_job_name" {
  type = string
}

variable "ses_from_email" {
  type = string
}

variable "notification_email" {
  type = string
}

variable "lambdas_source_dir" {
  description = "Path to the lambdas/ directory at the repo root"
  type        = string
}
