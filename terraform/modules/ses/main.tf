# ─────────────────────────────────────────────────────────────
# SES IDENTITIES
#
# SES starts in sandbox mode for new AWS accounts: you can only
# send TO and FROM verified identities. Both addresses below must
# be verified (check your inbox for the AWS verification email
# after `terraform apply` — click the link) before any notification
# will actually deliver. Terraform creates the identity and requests
# verification; it cannot complete verification for you, that
# requires clicking the email link AWS sends.
# ─────────────────────────────────────────────────────────────

resource "aws_ses_email_identity" "sender" {
  email = var.ses_from_email
}

resource "aws_ses_email_identity" "recipient" {
  # Only needed if recipient differs from sender and the account is
  # still in SES sandbox mode. If they're the same address, this is
  # a harmless duplicate identity request.
  email = var.notification_email
}

# ─────────────────────────────────────────────────────────────
# SNS TOPICS — one per pipeline stage
#
# Why SNS in front of SES instead of calling SES directly from
# each Lambda/Glue job: decouples "something happened" from "send
# an email about it". Glue jobs can publish to SNS via boto3 with
# zero IAM beyond sns:Publish — no SES permissions needed in the
# Glue role. A single Lambda subscribed to all three topics handles
# the actual SES send, so the email template lives in one place.
# ─────────────────────────────────────────────────────────────

resource "aws_sns_topic" "acquire_notifications" {
  name = "${var.project}-acquire-notifications"
  tags = { Project = var.project }
}

resource "aws_sns_topic" "landing_notifications" {
  name = "${var.project}-landing-notifications"
  tags = { Project = var.project }
}

resource "aws_sns_topic" "silver_notifications" {
  name = "${var.project}-silver-notifications"
  tags = { Project = var.project }
}
