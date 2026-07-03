output "acquire_function_name" {
  value = aws_lambda_function.acquire.function_name
}

output "acquire_function_arn" {
  value = aws_lambda_function.acquire.arn
}

output "trigger_glue_zip_function_name" {
  value = aws_lambda_function.trigger_glue_zip.function_name
}

output "trigger_glue_zip_function_arn" {
  value = aws_lambda_function.trigger_glue_zip.arn
}
