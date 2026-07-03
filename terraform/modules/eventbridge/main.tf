# ─────────────────────────────────────────────────────────────
# WHY EVENTBRIDGE INSTEAD OF DIRECT S3 BUCKET NOTIFICATION
#
# S3 → Lambda direct notifications (aws_s3_bucket_notification) work,
# but only one notification configuration can exist per bucket in
# Terraform state at a time, which gets fragile as the bucket grows
# more event-driven consumers later. Routing through EventBridge:
#   - Decouples the bucket from any specific consumer
#   - Allows multiple independent rules to react to the same S3 events
#     without fighting over a single notification block
#   - Gives a uniform trigger story: manual console drop, CLI `aws s3 cp`,
#     and the acquire Lambda's upload all produce the same EventBridge
#     event, so they're handled identically with one rule
#
# This requires turning on EventBridge notifications at the bucket
# level first (a one-line S3 bucket setting), then an EventBridge
# rule matches on the event detail.
# ─────────────────────────────────────────────────────────────

resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = var.lake_bucket_id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "zip_landed" {
  name        = "${var.project}-zip-landed"
  description = "Fires when any .zip is created under raw/ — covers acquire Lambda uploads AND manual console/CLI drops"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.lake_bucket_id]
      }
      object = {
        key = [{
          prefix = "raw/"
        }]
      }
    }
  })

  state = "ENABLED"
  tags  = { Project = var.project }

  depends_on = [aws_s3_bucket_notification.eventbridge]
}

resource "aws_cloudwatch_event_target" "zip_landed_target" {
  rule      = aws_cloudwatch_event_rule.zip_landed.name
  target_id = "TriggerGlueZipLambda"
  arn       = var.trigger_glue_zip_function_arn
}

resource "aws_lambda_permission" "allow_eventbridge_zip_landed" {
  statement_id  = "AllowEventBridgeZipLanded"
  action        = "lambda:InvokeFunction"
  function_name = var.trigger_glue_zip_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.zip_landed.arn
}