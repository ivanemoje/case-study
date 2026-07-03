variable "project" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "lake_bucket_id" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "glue_role_arn" {
  type = string
}

variable "processed_files_table_name" {
  type = string
}

variable "landing_topic_arn" {
  type = string
}

variable "silver_topic_arn" {
  type = string
}

variable "glue_jobs_source_dir" {
  description = "Path to the glue_jobs/ directory at the repo root"
  type        = string
}
