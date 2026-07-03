variable "project" {
  type = string
}

variable "notification_email" {
  description = "Email address to receive pipeline success/failure notifications. Must be verified in SES (sandbox mode) before emails will send."
  type        = string
}

variable "ses_from_email" {
  description = "Verified SES sender address. In sandbox mode this must also be a verified identity. Often the same as notification_email for a quick setup."
  type        = string
}
