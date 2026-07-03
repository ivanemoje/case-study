output "data_bucket_name" {
  value = module.s3.bucket_id
}

output "athena_results_bucket_name" {
  value = module.s3.athena_results_bucket_id
}

output "glue_database" {
  value = module.glue.database_name
}

output "zip_to_bronze_job_name" {
  value = module.glue.zip_to_bronze_job_name
}

output "bronze_to_silver_job_name" {
  value = module.glue.bronze_to_silver_job_name
}

output "athena_workgroup" {
  value = module.athena.workgroup_name
}

output "lambda_acquire_name" {
  value = module.lambda.acquire_function_name
}

output "lambda_trigger_glue_zip_name" {
  value = module.lambda.trigger_glue_zip_function_name
}

output "lambda_ses_sender_name" {
  value = module.lambda.ses_sender_function_name
}

output "processed_files_table_name" {
  value = module.s3.processed_files_table_name
}

output "ses_verification_reminder" {
  value = "IMPORTANT: Check the inbox for ${var.ses_from_email} and ${var.notification_email} — AWS sent verification emails. Click the links. Notifications will silently fail to send until both addresses are verified."
}
