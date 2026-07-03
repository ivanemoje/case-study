variable "project" {
  type = string
}

variable "notification_email" {
  description = "Email address subscribed directly to the pipeline SNS topic"
  type        = string
}
