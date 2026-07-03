"""Download the source ZIP, deduplicate it by SHA256, and place it in landing/."""

import hashlib
import logging
import os
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME = os.environ["BUCKET_NAME"]
SOURCE_URL = os.environ["SOURCE_URL"]
LEDGER_TABLE = os.environ["LEDGER_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
ledger = dynamodb.Table(LEDGER_TABLE)


def lambda_handler(event, context):
    run_ts = datetime.now(timezone.utc)
    run_str = run_ts.strftime("%Y%m%dT%H%M%SZ")
    local_path: Path | None = None

    try:
        logger.info("Acquire started at %s from %s", run_str, SOURCE_URL)
        local_path, checksum, size_bytes = _download_to_temp(SOURCE_URL)
        existing = _get_ledger_item(checksum)

        if existing and existing.get("status") in {
            "uploaded",
            "processing",
            "bronze_loaded",
            "completed",
        }:
            detail = (
                f"File checksum {checksum} is already registered.\n"
                f"Status: {existing.get('status')}\n"
                f"Landing key: {existing.get('landing_key', 'n/a')}\n"
                f"Archive key: {existing.get('archive_key', 'n/a')}\n"
                "No duplicate upload was made."
            )
            _notify("IATA Pipeline — Acquire SKIPPED", "SKIPPED", detail)
            return {
                "statusCode": 200,
                "status": "skipped_duplicate",
                "checksum": checksum,
            }

        landing_key = (
            f"landing/ingest_date={run_ts:%Y-%m-%d}/sales_{run_str}.zip"
        )
        _upload_file(local_path, landing_key)
        _write_ledger(checksum, landing_key, run_ts, size_bytes)

        detail = (
            "Downloaded and uploaded a new source archive.\n"
            f"Checksum: {checksum}\n"
            f"S3 key: {landing_key}\n"
            f"Size: {size_bytes} bytes\n"
            "The landing event will start the ZIP-to-bronze Glue job."
        )
        _notify("IATA Pipeline — Acquire SUCCEEDED", "SUCCESS", detail)

        return {
            "statusCode": 200,
            "status": "uploaded",
            "landing_key": landing_key,
            "checksum": checksum,
        }

    except Exception as exc:
        logger.exception("Acquire failed")
        _notify(
            "IATA Pipeline — Acquire FAILED",
            "FAILURE",
            f"Acquire failed at {run_str}.\n\nError: {exc}",
        )
        raise
    finally:
        if local_path:
            local_path.unlink(missing_ok=True)


def _download_to_temp(url: str) -> tuple[Path, str, int]:
    """Stream the ZIP to /tmp while calculating SHA256; do not hold it in memory."""
    digest = hashlib.sha256()
    size_bytes = 0

    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; IATA-Pipeline/1.0)"},
    )

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as target:
        local_path = Path(target.name)
        with urllib.request.urlopen(request, timeout=120) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status} from {url}")
            while chunk := response.read(8 * 1024 * 1024):
                target.write(chunk)
                digest.update(chunk)
                size_bytes += len(chunk)

    checksum = digest.hexdigest()
    logger.info("Downloaded %d bytes, SHA256=%s", size_bytes, checksum)
    return local_path, checksum, size_bytes


def _get_ledger_item(checksum: str) -> dict | None:
    try:
        return ledger.get_item(Key={"checksum_sha256": checksum}).get("Item")
    except Exception as exc:
        logger.error("Ledger lookup failed; proceeding as new data: %s", exc)
        return None


def _write_ledger(
    checksum: str,
    landing_key: str,
    uploaded_at: datetime,
    size_bytes: int,
) -> None:
    ledger.put_item(
        Item={
            "checksum_sha256": checksum,
            "landing_key": landing_key,
            "uploaded_at": uploaded_at.isoformat(),
            "size_bytes": size_bytes,
            "status": "uploaded",
        }
    )


def _upload_file(local_path: Path, key: str) -> None:
    s3.upload_file(
        str(local_path),
        BUCKET_NAME,
        key,
        ExtraArgs={
            "ContentType": "application/zip",
            "ServerSideEncryption": "AES256",
        },
    )
    logger.info("Uploaded s3://%s/%s", BUCKET_NAME, key)


def _notify(subject: str, status: str, detail: str) -> None:
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=(
                "Stage: acquire\n"
                f"Status: {status}\n"
                f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n\n"
                f"{detail}"
            ),
        )
    except Exception as exc:
        logger.error("SNS publish failed: %s", exc)
