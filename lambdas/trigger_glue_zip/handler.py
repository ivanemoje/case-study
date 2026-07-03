"""
lambdas/trigger_glue_zip/handler.py
─────────────────────────────────────
Triggered by EventBridge whenever an object is created under raw/
in the data lake bucket (covers acquire Lambda uploads AND manual
console/CLI drops — both produce the same S3 "Object Created" event
type, which EventBridge routes here identically).

Filters to .zip files only (EventBridge rule matches the raw/ prefix;
the suffix check happens here since cross-field S3 key suffix
matching in a single EventBridge rule is unreliable across event
schema versions).

Starts the zip_to_bronze Glue job, passing the S3 key as --RAW_KEY.
Does no data work itself — pure bridge.
"""

import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
BUCKET_NAME   = os.environ["BUCKET_NAME"]
RAW_PREFIX    = os.environ.get("RAW_PREFIX", "raw/")

glue = boto3.client("glue")


def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    key = _extract_key(event)

    if not key:
        logger.warning("Could not extract an S3 key from event — skipping")
        return {"statusCode": 200, "message": "no key found in event"}

    if not key.startswith(RAW_PREFIX):
        logger.info("Key %s not under %s — ignoring", key, RAW_PREFIX)
        return {"statusCode": 200, "message": "key outside watched prefix"}

    if not key.lower().endswith(".zip"):
        logger.info("Key %s is not a .zip — ignoring", key)
        return {"statusCode": 200, "message": "not a zip file"}

    logger.info("Starting Glue job %s with RAW_KEY=%s", GLUE_JOB_NAME, key)

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={"--RAW_KEY": key},
    )

    run_id = response["JobRunId"]
    logger.info("Glue job started: %s", run_id)

    return {
        "statusCode": 200,
        "job_name": GLUE_JOB_NAME,
        "job_run_id": run_id,
        "raw_key": key,
    }


def _extract_key(event: dict) -> str | None:
    """
    Supports three event shapes:
      1. EventBridge "Object Created" event (the normal path)
      2. Manual invocation payload: {"raw_key": "raw/sales.zip"}
      3. Legacy direct S3 notification (fallback, in case anyone
         wires this Lambda to S3 notifications directly instead)
    """
    if "raw_key" in event:
        return event["raw_key"]

    detail = event.get("detail", {})
    if detail:
        bucket = detail.get("bucket", {}).get("name")
        obj_key = detail.get("object", {}).get("key")
        if bucket == BUCKET_NAME and obj_key:
            return obj_key

    records = event.get("Records", [])
    if records and records[0].get("eventSource") == "aws:s3":
        return records[0]["s3"]["object"]["key"]

    return None
