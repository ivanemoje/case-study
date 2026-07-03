"""Start the ZIP-to-bronze Glue job for ZIP objects created under landing/."""

import json
import logging
import os
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
LANDING_PREFIX = os.environ.get("LANDING_PREFIX", "landing/")

glue = boto3.client("glue")


def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event))
    key = _extract_key(event)

    if not key:
        logger.warning("Could not extract an S3 key; skipping")
        return {"statusCode": 200, "message": "no key found"}

    key = unquote_plus(key)
    if not key.startswith(LANDING_PREFIX):
        logger.info("Ignoring key outside %s: %s", LANDING_PREFIX, key)
        return {"statusCode": 200, "message": "outside landing prefix"}

    if not key.lower().endswith(".zip"):
        logger.info("Ignoring non-ZIP key: %s", key)
        return {"statusCode": 200, "message": "not a ZIP file"}

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={"--LANDING_KEY": key},
    )
    run_id = response["JobRunId"]
    logger.info("Started %s run %s for %s", GLUE_JOB_NAME, run_id, key)

    return {
        "statusCode": 200,
        "job_name": GLUE_JOB_NAME,
        "job_run_id": run_id,
        "landing_key": key,
    }


def _extract_key(event: dict) -> str | None:
    if "landing_key" in event:
        return event["landing_key"]

    detail = event.get("detail", {})
    if detail:
        bucket = detail.get("bucket", {}).get("name")
        key = detail.get("object", {}).get("key")
        if bucket == BUCKET_NAME and key:
            return key

    records = event.get("Records", [])
    if records and records[0].get("eventSource") == "aws:s3":
        return records[0]["s3"]["object"]["key"]

    return None
