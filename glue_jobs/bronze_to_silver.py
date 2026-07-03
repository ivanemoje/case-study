"""
glue_jobs/bronze_to_silver.py
──────────────────────────────
Reads the bronze Iceberg table, quarantines rows that fail required
quality checks, deduplicates valid rows on order_id, casts/enriches
for silver, and refreshes the silver Iceberg table.

Design choices:
  - Missing order_id is fatal: those rows go to quarantine.
  - Missing region is recoverable: region becomes UNKNOWN and
    region_is_synthetic marks the row.
  - Silver is rebuilt from bronze each run. This avoids Glue/Spark
    MERGE limitations while keeping the output deterministic.
  - Silver DDL intentionally avoids NOT NULL constraints because
    Spark can preserve nullable=true metadata even after values are
    cleaned. Data quality is enforced in this job before writing.
"""

import json
import logging
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, TimestampType

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "GLUE_DATABASE",
    "BRONZE_TABLE",
    "SILVER_TABLE",
    "SILVER_PREFIX",
    "QUARANTINE_TABLE",
    "QUARANTINE_PREFIX",
    "SNS_TOPIC_ARN",
    "AWS_REGION",
])

BUCKET_NAME = args["BUCKET_NAME"]
GLUE_DATABASE = args["GLUE_DATABASE"]
BRONZE_TABLE = args["BRONZE_TABLE"]
SILVER_TABLE = args["SILVER_TABLE"]
SILVER_PREFIX = args["SILVER_PREFIX"]
QUARANTINE_TABLE = args["QUARANTINE_TABLE"]
QUARANTINE_PREFIX = args["QUARANTINE_PREFIX"]
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
AWS_REGION = args["AWS_REGION"]
WAREHOUSE = f"s3://{BUCKET_NAME}/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")

sns = boto3.client("sns", region_name=AWS_REGION)

SILVER_COLUMNS = [
    "region",
    "country",
    "item_type",
    "sales_channel",
    "order_priority",
    "order_date",
    "order_id",
    "ship_date",
    "units_sold",
    "unit_price",
    "unit_cost",
    "total_revenue",
    "total_cost",
    "total_profit",
    "order_year",
    "region_is_synthetic",
    "_ingested_at",
    "_source_file",
    "_transformed_at",
]

QUARANTINE_COLUMNS = [
    "region",
    "country",
    "item_type",
    "sales_channel",
    "order_priority",
    "order_date",
    "order_id",
    "ship_date",
    "units_sold",
    "unit_price",
    "unit_cost",
    "total_revenue",
    "total_cost",
    "total_profit",
    "_ingested_at",
    "_source_file",
    "_quarantine_reason",
    "_quarantined_at",
]

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{SILVER_TABLE} (
    region              STRING,
    country             STRING,
    item_type           STRING,
    sales_channel       STRING,
    order_priority      STRING,
    order_date          DATE,
    order_id            STRING,
    ship_date           DATE,
    units_sold          INT,
    unit_price          DOUBLE,
    unit_cost           DOUBLE,
    total_revenue       DOUBLE,
    total_cost          DOUBLE,
    total_profit        DOUBLE,
    order_year          INT,
    region_is_synthetic BOOLEAN,
    _ingested_at        TIMESTAMP,
    _source_file        STRING,
    _transformed_at     TIMESTAMP
)
USING iceberg
PARTITIONED BY (region, order_year)
LOCATION '{WAREHOUSE}{SILVER_PREFIX}'
TBLPROPERTIES (
    'write.format.default'             = 'parquet',
    'write.parquet.compression-codec'  = 'snappy',
    'write.target-file-size-bytes'     = '134217728',
    'format-version'                   = '2'
)
"""

QUARANTINE_DDL = f"""
CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{QUARANTINE_TABLE} (
    region              STRING,
    country             STRING,
    item_type           STRING,
    sales_channel       STRING,
    order_priority      STRING,
    order_date          STRING,
    order_id            STRING,
    ship_date           STRING,
    units_sold          STRING,
    unit_price          STRING,
    unit_cost           STRING,
    total_revenue       STRING,
    total_cost          STRING,
    total_profit        STRING,
    _ingested_at        TIMESTAMP,
    _source_file        STRING,
    _quarantine_reason  STRING,
    _quarantined_at     TIMESTAMP
)
USING iceberg
LOCATION '{WAREHOUSE}{QUARANTINE_PREFIX}'
TBLPROPERTIES (
    'write.format.default'             = 'parquet',
    'write.parquet.compression-codec'  = 'snappy',
    'format-version'                   = '2'
)
"""


def notify(status: str, detail: str) -> None:
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"IATA Pipeline — bronze_to_silver — {status}",
            Message=json.dumps({
                "stage": "bronze_to_silver",
                "status": status,
                "detail": detail,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }),
        )
    except Exception as exc:
        logger.error("SNS publish failed: %s", exc)


def read_bronze() -> DataFrame:
    table_name = f"glue_catalog.{GLUE_DATABASE}.{BRONZE_TABLE}"
    logger.info("Reading bronze table: %s", table_name)
    return spark.read.format("iceberg").load(table_name)


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    missing_order_id = F.col("order_id").isNull() | (F.trim(F.col("order_id")) == "")

    quarantined_df = (
        df.filter(missing_order_id)
        .withColumn("_quarantine_reason", F.lit("missing_order_id"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    clean_df = df.filter(~missing_order_id)
    return clean_df, quarantined_df


def write_quarantine(df: DataFrame) -> int:
    count = df.count()
    if count == 0:
        logger.info("No rows to quarantine")
        return 0

    spark.sql(QUARANTINE_DDL)

    output_df = (
        df
        .withColumn("region", F.col("region").cast(StringType()))
        .withColumn("country", F.col("country").cast(StringType()))
        .withColumn("item_type", F.col("item_type").cast(StringType()))
        .withColumn("sales_channel", F.col("sales_channel").cast(StringType()))
        .withColumn("order_priority", F.col("order_priority").cast(StringType()))
        .withColumn("order_date", F.col("order_date").cast(StringType()))
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("ship_date", F.col("ship_date").cast(StringType()))
        .withColumn("units_sold", F.col("units_sold").cast(StringType()))
        .withColumn("unit_price", F.col("unit_price").cast(StringType()))
        .withColumn("unit_cost", F.col("unit_cost").cast(StringType()))
        .withColumn("total_revenue", F.col("total_revenue").cast(StringType()))
        .withColumn("total_cost", F.col("total_cost").cast(StringType()))
        .withColumn("total_profit", F.col("total_profit").cast(StringType()))
        .withColumn("_ingested_at", F.col("_ingested_at").cast(TimestampType()))
        .withColumn("_source_file", F.col("_source_file").cast(StringType()))
        .withColumn("_quarantine_reason", F.col("_quarantine_reason").cast(StringType()))
        .withColumn("_quarantined_at", F.col("_quarantined_at").cast(TimestampType()))
        .select(*QUARANTINE_COLUMNS)
    )

    output_df.writeTo(f"glue_catalog.{GLUE_DATABASE}.{QUARANTINE_TABLE}").append()
    logger.warning("Quarantined %d rows", count)
    return count


def clean_region(df: DataFrame) -> DataFrame:
    missing_region = F.col("region").isNull() | (F.trim(F.col("region")) == "")
    return (
        df
        .withColumn("region_is_synthetic", missing_region.cast(BooleanType()))
        .withColumn(
            "region",
            F.when(missing_region, F.lit("UNKNOWN")).otherwise(F.col("region"))
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    from pyspark.sql.window import Window

    window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def cast_and_enrich(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("region", F.col("region").cast(StringType()))
        .withColumn("country", F.col("country").cast(StringType()))
        .withColumn("item_type", F.col("item_type").cast(StringType()))
        .withColumn("sales_channel", F.col("sales_channel").cast(StringType()))
        .withColumn("order_priority", F.col("order_priority").cast(StringType()))
        .withColumn("order_date", F.to_date(F.col("order_date"), "M/d/yyyy").cast(DateType()))
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("ship_date", F.to_date(F.col("ship_date"), "M/d/yyyy").cast(DateType()))
        .withColumn("units_sold", F.col("units_sold").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("unit_cost", F.col("unit_cost").cast(DoubleType()))
        .withColumn("total_revenue", F.col("total_revenue").cast(DoubleType()))
        .withColumn("total_cost", F.col("total_cost").cast(DoubleType()))
        .withColumn("total_profit", F.col("total_profit").cast(DoubleType()))
        .withColumn("order_year", F.coalesce(F.year(F.col("order_date")), F.lit(1970)).cast(IntegerType()))
        .withColumn("region_is_synthetic", F.col("region_is_synthetic").cast(BooleanType()))
        .withColumn("_ingested_at", F.col("_ingested_at").cast(TimestampType()))
        .withColumn("_source_file", F.col("_source_file").cast(StringType()))
        .withColumn("_transformed_at", F.current_timestamp().cast(TimestampType()))
        .select(*SILVER_COLUMNS)
    )


def refresh_silver(df: DataFrame) -> None:
    logger.info("Refreshing silver table without MERGE")

    spark.sql(f"DROP TABLE IF EXISTS glue_catalog.{GLUE_DATABASE}.{SILVER_TABLE}")
    spark.sql(SILVER_DDL)

    df.select(*SILVER_COLUMNS).writeTo(
        f"glue_catalog.{GLUE_DATABASE}.{SILVER_TABLE}"
    ).append()


def log_silver_summary() -> None:
    spark.sql(f"""
        SELECT region, order_year, COUNT(*) AS rows,
               SUM(CAST(region_is_synthetic AS INT)) AS synthetic_region_rows
        FROM glue_catalog.{GLUE_DATABASE}.{SILVER_TABLE}
        GROUP BY region, order_year
        ORDER BY region, order_year
    """).show(50, truncate=False)


def main() -> None:
    bronze_df = read_bronze()
    raw_count = bronze_df.count()
    logger.info("Bronze rows including duplicates: %d", raw_count)

    clean_df, quarantined_df = split_quarantine(bronze_df)
    clean_count = clean_df.count()
    quarantine_count = raw_count - clean_count
    logger.info("Clean rows: %d | Quarantined rows: %d", clean_count, quarantine_count)

    written_quarantine_count = write_quarantine(quarantined_df)

    clean_df = clean_region(clean_df)
    synthetic_region_count = clean_df.filter(F.col("region_is_synthetic") == True).count()
    if synthetic_region_count > 0:
        logger.warning("%d rows had missing region and were defaulted to UNKNOWN", synthetic_region_count)

    deduped_df = deduplicate(clean_df)
    dedup_count = deduped_df.count()
    logger.info("After dedup: %d rows (%d duplicates removed)", dedup_count, clean_count - dedup_count)

    silver_df = cast_and_enrich(deduped_df)
    refresh_silver(silver_df)
    log_silver_summary()

    msg = (
        f"Clean rows written to silver: {dedup_count}\n"
        f"Quarantined missing order_id rows: {written_quarantine_count}\n"
        f"Synthetic region rows defaulted to UNKNOWN: {synthetic_region_count}\n"
    )
    logger.info(msg)
    notify("SUCCESS", msg)


try:
    main()
    job.commit()
except Exception as exc:
    logger.exception("bronze_to_silver failed")
    notify("FAILURE", f"Error: {exc}")
    raise
