"""Rebuild the current silver and quarantine views from the bronze Iceberg table."""

import logging
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv,
    [
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
    ],
)

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
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
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
PARTITIONED BY (years(order_date))
LOCATION '{WAREHOUSE}{SILVER_PREFIX}'
TBLPROPERTIES (
    'format-version'                             = '2',
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '134217728',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10'
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
    'format-version'                             = '2',
    'write.format.default'                       = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.target-file-size-bytes'               = '134217728',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10'
)
"""


def notify(status: str, detail: str) -> None:
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"IATA Pipeline — Bronze to Silver — {status}",
            Message=(
                "Stage: bronze_to_silver\n"
                f"Status: {status}\n"
                f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n"
                f"{detail}"
            ),
        )
    except Exception as exc:
        logger.error("SNS publish failed: %s", exc)


def read_bronze() -> DataFrame:
    table_name = f"glue_catalog.{GLUE_DATABASE}.{BRONZE_TABLE}"
    logger.info("Reading %s", table_name)
    return spark.read.format("iceberg").load(table_name)


def missing_order_id_expression():
    return F.col("order_id").isNull() | (F.trim(F.col("order_id")) == "")


def build_quarantine(df: DataFrame) -> DataFrame:
    return (
        df.filter(missing_order_id_expression())
        .withColumn("_quarantine_reason", F.lit("missing_order_id"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


def cast_quarantine(df: DataFrame) -> DataFrame:
    string_columns = [
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
        "_source_file",
        "_quarantine_reason",
    ]
    output = df
    for column in string_columns:
        output = output.withColumn(column, F.col(column).cast(StringType()))
    return (
        output.withColumn("_ingested_at", F.col("_ingested_at").cast(TimestampType()))
        .withColumn("_quarantined_at", F.col("_quarantined_at").cast(TimestampType()))
        .select(*QUARANTINE_COLUMNS)
    )


def clean_and_deduplicate(df: DataFrame) -> DataFrame:
    valid = df.filter(~missing_order_id_expression())
    missing_region = F.col("region").isNull() | (F.trim(F.col("region")) == "")
    cleaned = (
        valid.withColumn("region_is_synthetic", missing_region.cast(BooleanType()))
        .withColumn(
            "region",
            F.when(missing_region, F.lit("UNKNOWN")).otherwise(F.trim(F.col("region"))),
        )
    )

    window = Window.partitionBy("order_id").orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    return (
        cleaned.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def cast_and_enrich(df: DataFrame) -> DataFrame:
    order_date = F.to_date(F.col("order_date"), "M/d/yyyy")
    return (
        df.withColumn("region", F.col("region").cast(StringType()))
        .withColumn("country", F.col("country").cast(StringType()))
        .withColumn("item_type", F.col("item_type").cast(StringType()))
        .withColumn("sales_channel", F.col("sales_channel").cast(StringType()))
        .withColumn("order_priority", F.col("order_priority").cast(StringType()))
        .withColumn("order_date", order_date.cast(DateType()))
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("ship_date", F.to_date(F.col("ship_date"), "M/d/yyyy").cast(DateType()))
        .withColumn("units_sold", F.col("units_sold").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("unit_cost", F.col("unit_cost").cast(DoubleType()))
        .withColumn("total_revenue", F.col("total_revenue").cast(DoubleType()))
        .withColumn("total_cost", F.col("total_cost").cast(DoubleType()))
        .withColumn("total_profit", F.col("total_profit").cast(DoubleType()))
        .withColumn("order_year", F.year(F.col("order_date")).cast(IntegerType()))
        .withColumn("region_is_synthetic", F.col("region_is_synthetic").cast(BooleanType()))
        .withColumn("_ingested_at", F.col("_ingested_at").cast(TimestampType()))
        .withColumn("_source_file", F.col("_source_file").cast(StringType()))
        .withColumn("_transformed_at", F.current_timestamp().cast(TimestampType()))
        .select(*SILVER_COLUMNS)
    )


def overwrite_table(df: DataFrame, table_name: str, view_name: str, columns: list[str]) -> None:
    df.createOrReplaceTempView(view_name)
    projection = ", ".join(columns)
    spark.sql(
        f"INSERT OVERWRITE TABLE glue_catalog.{GLUE_DATABASE}.{table_name} "
        f"SELECT {projection} FROM {view_name}"
    )


def main() -> None:
    bronze_df = read_bronze().persist(StorageLevel.MEMORY_AND_DISK)
    missing_order_id = missing_order_id_expression()
    missing_region = F.col("region").isNull() | (F.trim(F.col("region")) == "")

    metrics = bronze_df.agg(
        F.count(F.lit(1)).alias("raw_count"),
        F.sum(F.when(missing_order_id, 1).otherwise(0)).alias("quarantine_count"),
        F.sum(
            F.when((~missing_order_id) & missing_region, 1).otherwise(0)
        ).alias("synthetic_region_count"),
    ).first()

    raw_count = int(metrics["raw_count"] or 0)
    quarantine_count = int(metrics["quarantine_count"] or 0)
    clean_count = raw_count - quarantine_count
    synthetic_region_count = int(metrics["synthetic_region_count"] or 0)

    quarantine_df = cast_quarantine(build_quarantine(bronze_df))
    deduped_df = clean_and_deduplicate(bronze_df).persist(StorageLevel.MEMORY_AND_DISK)
    deduped_count = deduped_df.count()
    duplicate_count = clean_count - deduped_count
    silver_df = cast_and_enrich(deduped_df)

    spark.sql(SILVER_DDL)
    spark.sql(QUARANTINE_DDL)
    overwrite_table(quarantine_df, QUARANTINE_TABLE, "quarantine_output", QUARANTINE_COLUMNS)
    overwrite_table(silver_df, SILVER_TABLE, "silver_output", SILVER_COLUMNS)

    logger.info(
        "raw=%d clean=%d silver=%d duplicates_removed=%d quarantined=%d synthetic_region=%d",
        raw_count,
        clean_count,
        deduped_count,
        duplicate_count,
        quarantine_count,
        synthetic_region_count,
    )

    detail = (
        f"Bronze rows scanned: {raw_count}\n"
        f"Clean rows before deduplication: {clean_count}\n"
        f"Duplicate rows removed: {duplicate_count}\n"
        f"Rows written to silver: {deduped_count}\n"
        f"Rows in quarantine: {quarantine_count}\n"
        f"Synthetic region rows: {synthetic_region_count}"
    )
    notify("SUCCESS", detail)

    deduped_df.unpersist()
    bronze_df.unpersist()


try:
    main()
    job.commit()
except Exception as exc:
    logger.exception("bronze_to_silver failed")
    notify("FAILURE", f"Error: {exc}")
    raise
