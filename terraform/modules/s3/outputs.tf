output "bucket_id" {
  value = aws_s3_bucket.lake.id
}

output "bucket_arn" {
  value = aws_s3_bucket.lake.arn
}

output "athena_results_bucket_id" {
  value = aws_s3_bucket.athena_results.id
}

output "athena_results_bucket_arn" {
  value = aws_s3_bucket.athena_results.arn
}

output "processed_files_table_name" {
  value = aws_dynamodb_table.processed_files.name
}

output "processed_files_table_arn" {
  value = aws_dynamodb_table.processed_files.arn
}
