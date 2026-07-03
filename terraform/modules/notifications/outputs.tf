output "topic_arn" {
  value = aws_sns_topic.pipeline.arn
}

output "notification_email" {
  value = var.notification_email
}
