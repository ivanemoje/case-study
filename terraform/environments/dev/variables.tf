variable "aws_region" {
  type    = string
  default = "eu-central-2"
}

variable "project" {
  type    = string
  default = "iata-case-study"
}

variable "data_bucket_name" {
  description = "Main data lake bucket name — must be globally unique"
  type        = string
}

variable "athena_results_bucket_name" {
  description = "Athena results bucket name — must be globally unique"
  type        = string
}

variable "athena_results_expiry_days" {
  type    = number
  default = 3
}

variable "glue_database_name" {
  type    = string
  default = "iata_lake"
}

variable "source_data_url" {
  type    = string
  default = "https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip"
}

variable "notification_email" {
  description = "Email to receive pipeline notifications. Must verify via the SES confirmation email after first apply."
  type        = string
}

variable "ses_from_email" {
  description = "Verified SES sender address. Can be the same as notification_email."
  type        = string
}
