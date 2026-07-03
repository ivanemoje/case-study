locals {
  warehouse = "s3://${var.lake_bucket_id}/"

  iceberg_conf = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=${local.warehouse} --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
}

resource "aws_glue_catalog_database" "lake" {
  name        = var.glue_database_name
  description = "IATA case study medallion lake — bronze, silver, quarantine"
}

resource "aws_s3_object" "zip_to_bronze_script" {
  bucket = var.lake_bucket_id
  key    = "glue-scripts/zip_to_bronze.py"
  source = "${var.glue_jobs_source_dir}/zip_to_bronze.py"
  etag   = filemd5("${var.glue_jobs_source_dir}/zip_to_bronze.py")
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = var.lake_bucket_id
  key    = "glue-scripts/bronze_to_silver.py"
  source = "${var.glue_jobs_source_dir}/bronze_to_silver.py"
  etag   = filemd5("${var.glue_jobs_source_dir}/bronze_to_silver.py")
}

resource "aws_glue_job" "zip_to_bronze" {
  name              = "${var.project}-zip-to-bronze"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 60

  command {
    script_location = "s3://${var.lake_bucket_id}/glue-scripts/zip_to_bronze.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_conf
    "--BUCKET_NAME"                      = var.lake_bucket_id
    "--GLUE_DATABASE"                    = var.glue_database_name
    "--BRONZE_TABLE"                     = "sales_bronze"
    "--BRONZE_PREFIX"                    = "bronze/"
    "--RAW_KEY"                          = "raw/placeholder.zip"
    "--LEDGER_TABLE"                     = var.processed_files_table_name
    "--SNS_TOPIC_ARN"                    = var.landing_topic_arn
    "--AWS_REGION"                       = var.aws_region
    "--NEXT_JOB_NAME"                    = aws_glue_job.bronze_to_silver.name
  }

  tags = { Project = var.project }
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project}-bronze-to-silver"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    script_location = "s3://${var.lake_bucket_id}/glue-scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = ""
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_conf
    "--BUCKET_NAME"                      = var.lake_bucket_id
    "--GLUE_DATABASE"                    = var.glue_database_name
    "--BRONZE_TABLE"                     = "sales_bronze"
    "--SILVER_TABLE"                     = "sales_silver"
    "--SILVER_PREFIX"                    = "silver/"
    "--QUARANTINE_TABLE"                 = "sales_quarantine"
    "--QUARANTINE_PREFIX"                = "quarantine/"
    "--SNS_TOPIC_ARN"                    = var.silver_topic_arn
    "--AWS_REGION"                       = var.aws_region
  }

  tags = { Project = var.project }
}