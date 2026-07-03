"""Load a ZIP from landing/, append its CSV to bronze, then archive the ZIP."""

import hashlib
import logging
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BUCKET_NAME",
        "GLUE_DATABASE",
        "BRONZE_TABLE",
        "BRONZE_PREFIX",
        "LANDING_KEY",
        "ARCHIVE_PREFIX",
        "STAGING_PREFIX",
        "LEDGER_TABLE",
        "SNS_TOPIC_ARN",
        "AWS_REGION",
        "NEXT_JOB_NAME",
    ],
)

BUCKET_NAME = args["BUCKET_NAME"]
GLUE_DATABASE = args["GLUE_DATABASE"]
BRONZE_TABLE = args["BRONZE_TABLE"]
BRONZE_PREFIX = args["BRONZE_PREFIX"]
LANDING_KEY = args["LANDING_KEY"]
ARCHIVE_PREFIX = args["ARCHIVE_PREFIX"].rstrip("/") + "/"
STAGING_PREFIX = args["STAGING_PREFIX"].rstrip("/") + "/"
LEDGER_TABLE = args["LEDGER_TABLE"]
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
AWS_REGION = args["AWS_REGION"]
NEXT_JOB_NAME = args["NEXT_JOB_NAME"]
WAREHOUSE = f"s3://{BUCKET_NAME}/"

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
sns = boto3.client("sns", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)
ledger = dynamodb.Table(LEDGER_TABLE)

COLUMN_MAP = {
    "Region": "region",
    "Country": "country",
    "Item Type": "item_type",
    "Sales Channel": "sales_channel",
    "Order Priority": "order_priority",
    "Order Date": "order_date",
    "Order ID": "order_id",
    "Ship Date": "ship_date",
    "Units Sold": "units_sold",
    "Unit Price": "unit_price",
    "Unit Cost": "unit_cost",
    "Total Revenue": "total_revenue",
    "Total Cost": "total_cost",
    "Total Profit": "total_profit",
}

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{BRONZE_TABLE} (
    region           STRING,
    country          STRING,
    item_type        STRING,
    sales_channel    STRING,
    order_priority   STRING,
    order_date       STRING,
    order_id         STRING,
    ship_date        STRING,
    units_sold       STRING,
    unit_price       STRING,
    unit_cost        STRING,
    total_revenue    STRING,
    total_cost       STRING,
    total_profit     STRING,
    _ingested_at     TIMESTAMP,
    _source_file     STRING,
    _checksum_sha256 STRING
)
USING iceberg
LOCATION '{WAREHOUSE}{BRONZE_PREFIX}'
TBLPROPERTIES (
    'format-version'                            = '2',
    'write.format.default'                      = 'parquet',
    'write.parquet.compression-codec'           = 'snappy',
    'write.target-file-size-bytes'              = '134217728',
    'write.metadata.delete-after-commit.enabled'= 'true',
    'write.metadata.previous-versions-max'      = '10'
)
"""


def notify(status: str, detail: str) -> None:
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"IATA Pipeline — ZIP to Bronze — {status}",
            Message=(
                "Stage: zip_to_bronze\n"
                f"Status: {status}\n"
                f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n"
                f"{detail}"
            ),
        )
    except Exception as exc:
        logger.error("SNS publish failed: %s", exc)


def download_and_checksum(key: str, directory: Path) -> tuple[Path, str]:
    local_zip = directory / "source.zip"
    logger.info("Downloading s3://%s/%s", BUCKET_NAME, key)
    s3.download_file(BUCKET_NAME, key, str(local_zip))

    digest = hashlib.sha256()
    with local_zip.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)

    checksum = digest.hexdigest()
    logger.info("Downloaded %d bytes, SHA256=%s", local_zip.stat().st_size, checksum)
    return local_zip, checksum


def get_ledger_item(checksum: str) -> dict | None:
    try:
        return ledger.get_item(Key={"checksum_sha256": checksum}).get("Item")
    except Exception as exc:
        logger.error("Ledger lookup failed; continuing: %s", exc)
        return None


def update_ledger(checksum: str, **values) -> None:
    if not values:
        return

    names = {f"#{key}": key for key in values}
    expression_values = {f":{key}": value for key, value in values.items()}
    assignments = ", ".join(f"#{key} = :{key}" for key in values)
    ledger.update_item(
        Key={"checksum_sha256": checksum},
        UpdateExpression=f"SET {assignments}",
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=expression_values,
    )


def extract_csv(local_zip: Path, directory: Path) -> Path:
    with zipfile.ZipFile(local_zip) as archive:
        csv_names = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
            and not name.startswith("__MACOSX/")
            and not name.endswith("/")
        ]
        if not csv_names:
            raise ValueError(f"No CSV found inside {LANDING_KEY}")
        if len(csv_names) > 1:
            logger.warning("Multiple CSVs found; using %s", csv_names[0])

        local_csv = directory / "source.csv"
        with archive.open(csv_names[0]) as source, local_csv.open("wb") as target:
            shutil.copyfileobj(source, target, length=8 * 1024 * 1024)

    logger.info("Extracted %d bytes to %s", local_csv.stat().st_size, local_csv)
    return local_csv


def upload_staging_csv(local_csv: Path, checksum: str) -> str:
    staging_key = f"{STAGING_PREFIX}checksum={checksum}/{Path(LANDING_KEY).stem}.csv"
    s3.upload_file(
        str(local_csv),
        BUCKET_NAME,
        staging_key,
        ExtraArgs={"ContentType": "text/csv", "ServerSideEncryption": "AES256"},
    )
    logger.info("Uploaded staging CSV to s3://%s/%s", BUCKET_NAME, staging_key)
    return staging_key


def delete_object(key: str) -> None:
    s3.delete_object(Bucket=BUCKET_NAME, Key=key)
    logger.info("Deleted s3://%s/%s", BUCKET_NAME, key)


def archive_landing_object(checksum: str) -> str:
    date_part = next(
        (
            part.split("=", 1)[1]
            for part in LANDING_KEY.split("/")
            if part.startswith("ingest_date=")
        ),
        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    filename = os.path.basename(LANDING_KEY)
    archive_key = (
        f"{ARCHIVE_PREFIX}ingest_date={date_part}/"
        f"{checksum[:12]}_{filename}"
    )
    s3.copy_object(
        Bucket=BUCKET_NAME,
        Key=archive_key,
        CopySource={"Bucket": BUCKET_NAME, "Key": LANDING_KEY},
        ServerSideEncryption="AES256",
        MetadataDirective="COPY",
    )
    delete_object(LANDING_KEY)
    logger.info("Archived landing object to s3://%s/%s", BUCKET_NAME, archive_key)
    return archive_key



def start_next_job() -> str:
    response = glue.start_job_run(JobName=NEXT_JOB_NAME)
    run_id = response["JobRunId"]
    logger.info("Started %s run %s", NEXT_JOB_NAME, run_id)
    return run_id


def build_bronze_dataframe(staging_key: str, checksum: str):
    source_path = f"s3://{BUCKET_NAME}/{staging_key}"
    raw_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .csv(source_path)
    )

    df = raw_df
    for source_name, target_name in COLUMN_MAP.items():
        if source_name in df.columns:
            df = df.withColumnRenamed(source_name, target_name)
        else:
            logger.warning("Missing source column %s; filling with NULL", source_name)
            df = df.withColumn(target_name, F.lit(None).cast(StringType()))

    columns = list(COLUMN_MAP.values()) + [
        "_ingested_at",
        "_source_file",
        "_checksum_sha256",
    ]
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(LANDING_KEY))
        .withColumn("_checksum_sha256", F.lit(checksum))
        .select(*columns)
    )


def main() -> None:
    logger.info("zip_to_bronze started for LANDING_KEY=%s", LANDING_KEY)
    staging_key: str | None = None

    with tempfile.TemporaryDirectory(prefix="iata-") as temp_dir:
        directory = Path(temp_dir)
        local_zip, checksum = download_and_checksum(LANDING_KEY, directory)
        existing = get_ledger_item(checksum)

        if existing and existing.get("status") == "completed":
            delete_object(LANDING_KEY)
            detail = (
                f"Checksum {checksum} was already completed.\n"
                f"Original archive: {existing.get('archive_key', 'n/a')}\n"
                "The duplicate landing object was deleted; bronze was not changed."
            )
            logger.info(detail)
            notify("SKIPPED", detail)
            return

        if existing and existing.get("status") == "bronze_loaded":
            silver_run_id = start_next_job()
            archive_key = archive_landing_object(checksum)
            update_ledger(
                checksum,
                status="completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                archive_key=archive_key,
                silver_job_run_id=silver_run_id,
            )
            notify(
                "RECOVERED",
                f"Bronze had already been loaded. Silver run {silver_run_id} was started and the landing ZIP was archived.",
            )
            return

        update_ledger(
            checksum,
            status="processing",
            landing_key=LANDING_KEY,
            processing_started_at=datetime.now(timezone.utc).isoformat(),
        )

        try:
            local_csv = extract_csv(local_zip, directory)
            staging_key = upload_staging_csv(local_csv, checksum)
            bronze_df = build_bronze_dataframe(staging_key, checksum).cache()

            metrics = bronze_df.agg(
                F.count(F.lit(1)).alias("row_count"),
                F.sum(
                    F.when(
                        F.col("region").isNull() | (F.trim(F.col("region")) == ""),
                        1,
                    ).otherwise(0)
                ).alias("missing_region"),
                F.sum(
                    F.when(
                        F.col("order_id").isNull()
                        | (F.trim(F.col("order_id")) == ""),
                        1,
                    ).otherwise(0)
                ).alias("missing_order_id"),
            ).first()

            row_count = int(metrics["row_count"] or 0)
            missing_region = int(metrics["missing_region"] or 0)
            missing_order_id = int(metrics["missing_order_id"] or 0)

            spark.sql(BRONZE_DDL)
            bronze_df.writeTo(
                f"glue_catalog.{GLUE_DATABASE}.{BRONZE_TABLE}"
            ).append()
            bronze_df.unpersist()

            update_ledger(
                checksum,
                status="bronze_loaded",
                bronze_loaded_at=datetime.now(timezone.utc).isoformat(),
                bronze_row_count=row_count,
                landing_key=LANDING_KEY,
            )

            silver_run_id = start_next_job()
            archive_key = archive_landing_object(checksum)
            update_ledger(
                checksum,
                status="completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                archive_key=archive_key,
                silver_job_run_id=silver_run_id,
            )

            detail = (
                f"Loaded {row_count} rows from {LANDING_KEY}.\n"
                f"Archive key: {archive_key}\n"
                f"Checksum: {checksum}\n"
                f"Missing region: {missing_region}\n"
                f"Missing order_id: {missing_order_id}\n"
                f"Silver job run: {silver_run_id}"
            )
            notify("SUCCESS", detail)
        finally:
            if staging_key:
                try:
                    delete_object(staging_key)
                except Exception as cleanup_exc:
                    logger.warning("Could not delete staging object %s: %s", staging_key, cleanup_exc)


try:
    main()
    job.commit()
except Exception as exc:
    logger.exception("zip_to_bronze failed")
    notify("FAILURE", f"LANDING_KEY={LANDING_KEY}\n\nError: {exc}")
    raise
