"""
lambdas/acquire/handler.py
────────────────────────────
1. Downloads the zip from SOURCE_URL (stays compressed — no extraction here)
2. Computes SHA256 of the zip bytes
3. Checks the DynamoDB ledger — if this checksum was already processed,
   skips the upload entirely and publishes a "skipped" notification
4. Otherwise uploads the zip as-is to raw/<original-or-derived-name>.zip
   and writes a ledger entry (status=uploaded — the zip_to_bronze Glue
   job updates this to status=completed once it succeeds)
5. Publishes a result message to SNS (success or failure) for the
   ses_sender Lambda to turn into an email

Why the zip is NOT extracted here: CSV extraction for "big data" sized
files belongs in Glue, which has Spark's distributed memory management
instead of Lambda's fixed memory/disk ceiling. Lambda's only job is to
get the file into S3 reliably and record what it did.
"""

import boto3
import hashlib
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME   = os.environ["BUCKET_NAME"]
SOURCE_URL    = os.environ["SOURCE_URL"]
LEDGER_TABLE  = os.environ["LEDGER_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

s3        = boto3.client("s3")
dynamodb  = boto3.resource("dynamodb")
sns       = boto3.client("sns")
ledger    = dynamodb.Table(LEDGER_TABLE)


def lambda_handler(event, context):
    run_ts  = datetime.now(timezone.utc)
    run_str = run_ts.strftime("%Y%m%dT%H%M%SZ")

    try:
        logger.info("Acquire started: %s  source: %s", run_str, SOURCE_URL)

        zip_bytes = _download(SOURCE_URL)
        checksum  = hashlib.sha256(zip_bytes).hexdigest()
        logger.info("Downloaded %d bytes, SHA256=%s", len(zip_bytes), checksum)

        existing = _check_ledger(checksum)
        if existing:
            logger.info("Checksum already processed at %s — skipping upload",
                        existing.get("processed_at"))
            _notify(
                subject="IATA Pipeline — Acquire SKIPPED (duplicate)",
                message=(
                    f"File with checksum {checksum} was already processed.\n"
                    f"Original upload: {existing.get('raw_key')}\n"
                    f"Originally processed at: {existing.get('processed_at')}\n"
                    f"No new upload was made."
                ),
                status="SKIPPED",
            )
            return {
                "statusCode": 200,
                "status": "skipped_duplicate",
                "checksum": checksum,
                "existing_raw_key": existing.get("raw_key"),
            }

        raw_key = f"raw/sales_{run_str}.zip"
        _put_s3(raw_key, zip_bytes)
        logger.info("Uploaded → s3://%s/%s", BUCKET_NAME, raw_key)

        _write_ledger(checksum, raw_key, run_ts)

        _notify(
            subject="IATA Pipeline — Acquire SUCCEEDED",
            message=(
                f"Downloaded and uploaded new file.\n"
                f"Checksum: {checksum}\n"
                f"S3 key: {raw_key}\n"
                f"Size: {len(zip_bytes)} bytes\n"
                f"This will automatically trigger the zip-to-bronze Glue job "
                f"via EventBridge."
            ),
            status="SUCCESS",
        )

        return {
            "statusCode": 200,
            "status": "uploaded",
            "raw_key": raw_key,
            "checksum": checksum,
        }

    except Exception as exc:
        logger.exception("Acquire failed")
        _notify(
            subject="IATA Pipeline — Acquire FAILED",
            message=f"Acquire Lambda failed at {run_str}.\n\nError: {exc}",
            status="FAILURE",
        )
        raise


def _download(url: str) -> bytes:
    logger.info("Fetching %s", url)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; IATA-Pipeline/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} from {url}")
        return r.read()


def _check_ledger(checksum: str) -> dict | None:
    try:
        response = ledger.get_item(Key={"checksum_sha256": checksum})
        return response.get("Item")
    except Exception as exc:
        logger.error("Ledger check failed (treating as not-processed): %s", exc)
        return None


def _write_ledger(checksum: str, raw_key: str, processed_at: datetime) -> None:
    ledger.put_item(Item={
        "checksum_sha256": checksum,
        "raw_key":         raw_key,
        "processed_at":    processed_at.isoformat(),
        "status":          "uploaded",
    })


def _put_s3(key: str, body: bytes) -> None:
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body,
        ContentType="application/zip",
        ServerSideEncryption="AES256",
    )


def _notify(subject: str, message: str, status: str) -> None:
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=json.dumps({
                "stage": "acquire",
                "status": status,
                "detail": message,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }),
        )
    except Exception as exc:
        # Notification failure should never fail the pipeline itself
        logger.error("SNS publish failed: %s", exc)
