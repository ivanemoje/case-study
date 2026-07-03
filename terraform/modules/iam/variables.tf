variable "project" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "athena_results_bucket_arn" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "processed_files_table_arn" {
  type = string
}

variable "notify_topic_arns" {
  description = "List of SNS topic ARNs that Lambdas/Glue jobs need sns:Publish on"
  type        = list(string)
}
