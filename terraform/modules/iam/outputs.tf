output "lambda_acquire_role_arn" {
  value = aws_iam_role.lambda_acquire.arn
}

output "lambda_trigger_glue_role_arn" {
  value = aws_iam_role.lambda_trigger_glue.arn
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}
