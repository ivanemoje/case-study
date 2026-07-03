output "acquire_topic_arn" {
  value = aws_sns_topic.acquire_notifications.arn
}

output "landing_topic_arn" {
  value = aws_sns_topic.landing_notifications.arn
}

output "silver_topic_arn" {
  value = aws_sns_topic.silver_notifications.arn
}

output "ses_from_email" {
  value = aws_ses_email_identity.sender.email
}

output "notification_email" {
  value = aws_ses_email_identity.recipient.email
}
