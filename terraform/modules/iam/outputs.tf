output "lambda_acquire_role_arn" {
  value = aws_iam_role.lambda_acquire.arn
}

output "lambda_trigger_glue_role_arn" {
  value = aws_iam_role.lambda_trigger_glue.arn
}

output "lambda_ses_sender_role_arn" {
  value = aws_iam_role.lambda_ses_sender.arn
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}
