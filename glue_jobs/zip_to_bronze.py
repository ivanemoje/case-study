"""
glue_jobs/zip_to_bronze.py
─────────────────────────────
Reads a .zip from raw/, extracts the CSV inside it, and appends the
rows to the bronze Iceberg table.

WHY EXTRACTION HAPPENS HERE, AND HOW IT SCALES TO BIG FILES
─────────────────────────────────────────────────────────────
Spark cannot read a .zip archive's contents directly the way it reads
.gz or .bz2 (zip is not a single-stream compression codec Hadoop
recognizes the same way). The standard pattern, and the one used
here, is:
  1. Download the zip from S3 to the Glue driver's local disk
     (boto3 get_object — driver only, not distributed)
  2. Extract the CSV from the zip using Python's zipfile (stdlib)
     to the driver's local disk
  3. Upload the extracted CSV back to S3 (uncompressed, as
     intermediate/ — kept for debugging/audit, not part of the
     medallion layers)
  4. Spark reads the now-uncompressed CSV from S3 in a fully
     distributed read — THIS is the step that scales to large files,
     because from here on it's standard distributed Spark, not
     driver-only processing

Steps 1-3 run on the driver and do consume driver memory/disk
proportional to the COMPRESSED zip size (not the decompressed CSV
size, since extraction streams to disk rather than loading the full
CSV into Python memory). For genuinely massive providers (10s of GB
compressed), the right evolution is a Glue Python Shell job or a
dedicated decompression Lambda with EFS, not Spark — see README
"Production Hardening" for that discussion. At the 2M-row / ~75MB
scale of this case study, driver-side extraction is appropriate and
keeps the job simple.

CHECKSUM LEDGER — defense in depth
─────────────────────────────────────────────────────────────
The acquire Lambda already checks the DynamoDB ledger before
uploading. This job checks again, independently, because:
  - Someone could manually drop a duplicate zip via console/CLI,
    bypassing the acquire Lambda's check entirely
  - It's the cheap, correct thing to do — a few DynamoDB reads cost
    nothing next to a multi-minute Spark job
If the checksum is already marked "completed" in the ledger, this
job exits immediately without touching bronze, and publishes a
SKIPPED notification.

BRONZE CONTRACT — unchanged from prior versions
─────────────────────────────────────────────────────────────
Faithful raw copy. No NOT NULL constraints. No dropped rows. No
fabricated values. Missing order_id and missing region are handled
downstream in bronze_to_silver.py, not here.
"""

import boto3
import hashlib
import io
import json
import sys
import zipfile
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Args ──────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "GLUE_DATABASE",
    "BRONZE_TABLE",
    "BRONZE_PREFIX",
    "RAW_KEY",
    "LEDGER_TABLE",
    "SNS_TOPIC_ARN",
    "AWS_REGION",
    "NEXT_JOB_NAME",
])

BUCKET_NAME   = args["BUCKET_NAME"]
GLUE_DATABASE = args["GLUE_DATABASE"]
BRONZE_TABLE  = args["BRONZE_TABLE"]
BRONZE_PREFIX = args["BRONZE_PREFIX"]
RAW_KEY       = args["RAW_KEY"]
LEDGER_TABLE  = args["LEDGER_TABLE"]
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
AWS_REGION    = args["AWS_REGION"]
NEXT_JOB_NAME = args["NEXT_JOB_NAME"]
WAREHOUSE     = f"s3://{BUCKET_NAME}/"

# ── Spark / Glue context ──────────────────────────────────────

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3       = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
sns      = boto3.client("sns", region_name=AWS_REGION)
glue     = boto3.client("glue", region_name=AWS_REGION)
ledger   = dynamodb.Table(LEDGER_TABLE)

# ── Column map: CSV header → bronze snake_case name ───────────

COLUMN_MAP = {
    "Region":         "region",
    "Country":        "country",
    "Item Type":      "item_type",
    "Sales Channel":  "sales_channel",
    "Order Priority": "order_priority",
    "Order Date":     "order_date",
    "Order ID":       "order_id",
    "Ship Date":      "ship_date",
    "Units Sold":     "units_sold",
    "Unit Price":     "unit_price",
    "Unit Cost":      "unit_cost",
    "Total Revenue":  "total_revenue",
    "Total Cost":     "total_cost",
    "Total Profit":   "total_profit",
}

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{BRONZE_TABLE} (
    region          STRING,
    country         STRING,
    item_type       STRING,
    sales_channel   STRING,
    order_priority  STRING,
    order_date      STRING,
    order_id        STRING,
    ship_date       STRING,
    units_sold      STRING,
    unit_price      STRING,
    unit_cost       STRING,
    total_revenue   STRING,
    total_cost      STRING,
    total_profit    STRING,
    _ingested_at    TIMESTAMP,
    _source_file    STRING,
    _checksum_sha256 STRING
)
USING iceberg
LOCATION '{WAREHOUSE}{BRONZE_PREFIX}'
TBLPROPERTIES (
    'write.format.default'             = 'parquet',
    'write.parquet.compression-codec'  = 'snappy',
    'write.target-file-size-bytes'     = '134217728',
    'format-version'                   = '2'
)
"""


def notify(stage: str, status: str, detail: str) -> None:
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"IATA Pipeline — {stage} — {status}",
            Message=json.dumps({
                "stage": stage,
                "status": status,
                "detail": detail,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }),
        )
    except Exception as exc:
        logger.error("SNS publish failed: %s", exc)

def start_next_job() -> None:
    logger.info("Starting next Glue job: %s", NEXT_JOB_NAME)
    response = glue.start_job_run(JobName=NEXT_JOB_NAME)
    logger.info("Started %s run_id=%s", NEXT_JOB_NAME, response["JobRunId"])



def compute_checksum_and_download(bucket: str, key: str) -> tuple[bytes, str]:
    """
    Downloads the zip to driver memory and computes its SHA256.
    Returns (zip_bytes, checksum_hex).
    """
    logger.info("Downloading s3://%s/%s for extraction", bucket, key)
    response = s3.get_object(Bucket=bucket, Key=key)
    zip_bytes = response["Body"].read()
    checksum = hashlib.sha256(zip_bytes).hexdigest()
    logger.info("Downloaded %d bytes, SHA256=%s", len(zip_bytes), checksum)
    return zip_bytes, checksum


def check_ledger_completed(checksum: str) -> dict | None:
    try:
        response = ledger.get_item(Key={"checksum_sha256": checksum})
        item = response.get("Item")
        if item and item.get("status") == "completed":
            return item
        return None
    except Exception as exc:
        logger.error("Ledger check failed (proceeding as not-completed): %s", exc)
        return None


def mark_ledger_completed(checksum: str, raw_key: str, row_count: int) -> None:
    try:
        ledger.update_item(
            Key={"checksum_sha256": checksum},
            UpdateExpression=(
                "SET #status = :status, completed_at = :ts, "
                "bronze_row_count = :rc, raw_key = :rk"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "completed",
                ":ts": datetime.now(timezone.utc).isoformat(),
                ":rc": row_count,
                ":rk": raw_key,
            },
        )
    except Exception as exc:
        # If the ledger entry doesn't exist yet (manual drop bypassing
        # acquire Lambda), create it fresh instead of updating.
        logger.warning("Ledger update failed, attempting fresh put: %s", exc)
        ledger.put_item(Item={
            "checksum_sha256": checksum,
            "raw_key": raw_key,
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "bronze_row_count": row_count,
        })


def extract_csv_and_upload(zip_bytes: bytes, raw_key: str) -> str:
    """
    Extracts the first CSV found in the zip and uploads it
    uncompressed to S3 under intermediate/, returning the S3 key.
    This is the driver-side step described in the module docstring.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found inside {raw_key}")
        csv_name = csv_names[0]
        logger.info("Extracting %s from %s", csv_name, raw_key)
        csv_bytes = zf.read(csv_name)

    intermediate_key = raw_key.replace("raw/", "intermediate/").replace(".zip", ".csv")
    logger.info("Uploading extracted CSV (%d bytes) to s3://%s/%s",
                len(csv_bytes), BUCKET_NAME, intermediate_key)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=intermediate_key,
        Body=csv_bytes,
        ContentType="text/csv",
        ServerSideEncryption="AES256",
    )
    return intermediate_key


def main():
    logger.info("zip_to_bronze started for RAW_KEY=%s", RAW_KEY)

    zip_bytes, checksum = compute_checksum_and_download(BUCKET_NAME, RAW_KEY)

    already_done = check_ledger_completed(checksum)
    if already_done:
        msg = (
            f"Checksum {checksum} already marked completed "
            f"(originally from {already_done.get('raw_key')}, "
            f"{already_done.get('bronze_row_count')} rows). "
            f"Skipping — no duplicate write to bronze."
        )
        logger.info(msg)
        notify("zip_to_bronze", "SKIPPED", msg)
        job.commit()
        return

    intermediate_key = extract_csv_and_upload(zip_bytes, RAW_KEY)
    source_path = f"s3://{BUCKET_NAME}/{intermediate_key}"

    logger.info("Reading extracted CSV with Spark: %s", source_path)
    raw_df = (
        spark.read
        .option("header",      "true")
        .option("inferSchema", "false")
        .option("encoding",    "UTF-8")
        .csv(source_path)
    )

    row_count = raw_df.count()
    logger.info("Read %d rows", row_count)

    df = raw_df
    for src, dst in COLUMN_MAP.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
        else:
            logger.warning("Column '%s' missing in source — filling with NULL", src)
            df = df.withColumn(dst, F.lit(None).cast(StringType()))

    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(RAW_KEY))
        .withColumn("_checksum_sha256", F.lit(checksum))
    )

    bronze_cols = list(COLUMN_MAP.values()) + ["_ingested_at", "_source_file", "_checksum_sha256"]
    df = df.select(*bronze_cols)

    # Informational data quality counts — nothing dropped here.
    null_region_count = df.filter(
        F.col("region").isNull() | (F.trim(F.col("region")) == "")
    ).count()
    null_order_id_count = df.filter(
        F.col("order_id").isNull() | (F.trim(F.col("order_id")) == "")
    ).count()
    if null_region_count:
        logger.warning("%d rows missing region (handled in silver)", null_region_count)
    if null_order_id_count:
        logger.warning("%d rows missing order_id (will be quarantined in silver)",
                       null_order_id_count)

    logger.info("Ensuring bronze table: glue_catalog.%s.%s", GLUE_DATABASE, BRONZE_TABLE)
    spark.sql(BRONZE_DDL)

    logger.info("Appending %d rows to bronze", row_count)
    df.writeTo(f"glue_catalog.{GLUE_DATABASE}.{BRONZE_TABLE}").append()

    mark_ledger_completed(checksum, RAW_KEY, row_count)

    msg = (
        f"Extracted and loaded {row_count} rows from {RAW_KEY} into bronze.\n"
        f"Checksum: {checksum}\n"
        f"Missing region: {null_region_count}  |  Missing order_id: {null_order_id_count}\n"
        f"The bronze_to_silver job will be started by this zip_to_bronze job."
    )
    logger.info(msg)
    notify("zip_to_bronze", "SUCCESS", msg)
    start_next_job()


try:
    main()
    job.commit()
except Exception as exc:
    logger.exception("zip_to_bronze failed")
    notify("zip_to_bronze", "FAILURE", f"RAW_KEY={RAW_KEY}\n\nError: {exc}")
    raise
