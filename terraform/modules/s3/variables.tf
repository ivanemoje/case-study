variable "project" {
  type = string
}

variable "data_bucket_name" {
  description = "Main data lake bucket name — globally unique"
  type        = string
}

variable "athena_results_bucket_name" {
  description = "Separate bucket for Athena query results — globally unique"
  type        = string
}

variable "athena_results_expiry_days" {
  description = "Days before Athena result objects are deleted"
  type        = number
  default     = 3
}
