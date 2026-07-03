output "database_name" {
  value = aws_glue_catalog_database.lake.name
}

output "zip_to_bronze_job_name" {
  value = aws_glue_job.zip_to_bronze.name
}

output "bronze_to_silver_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}
