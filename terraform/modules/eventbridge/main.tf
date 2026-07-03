resource "aws_s3_bucket_notification" "eventbridge" {
  bucket      = var.lake_bucket_id
  eventbridge = true
}

resource "aws_cloudwatch_event_rule" "zip_landed" {
  name        = "${var.project}-zip-landed"
  description = "Start ingestion when a ZIP is created under landing/"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.lake_bucket_id] }
      object = {
        key = [{ prefix = "landing/" }]
      }
    }
  })

  state      = "ENABLED"
  tags       = { Project = var.project }
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
