resource "aws_athena_workgroup" "lake" {
  name  = "${var.project}-workgroup"
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      # Separate bucket — see s3 module for why. Lifecycle expiry
      # there cleans these up automatically, no cost accumulation.
      output_location = "s3://${var.athena_results_bucket_id}/results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }

  tags = { Project = var.project }
}
