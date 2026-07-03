resource "aws_sns_topic" "pipeline" {
  name = "${var.project}-pipeline-notifications"
  tags = { Project = var.project }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline.arn
  protocol  = "email"
  endpoint  = var.notification_email
}
