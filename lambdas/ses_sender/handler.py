"""
lambdas/ses_sender/handler.py
────────────────────────────────
Subscribed to all three SNS topics (acquire, landing/zip_to_bronze,
silver/bronze_to_silver). Each publisher (acquire Lambda, the two
Glue jobs) sends a JSON message with stage/status/detail/timestamp.
This Lambda's only job is to format that into an email and send it
via SES.
Centralising the email template here means:
  - One place to change subject lines, formatting, branding
  - Glue jobs only need sns:Publish IAM permission, not ses:SendEmail
  - Easy to add Slack/PagerDuty/etc as additional SNS subscribers
    later without touching the Glue jobs or acquire Lambda at all
SES sandbox note: both SES_FROM_EMAIL and NOTIFICATION_EMAIL must be
verified identities (AWS sends a confirmation link to each on
`terraform apply` — click it) or SES will reject the send with a
"not verified" error, which this Lambda logs but does not retry,
since persistent retries on the same Lambda invocation is the
SNS->Lambda subscription's job (SNS retries deliveries to Lambda on
failure based on its own backoff, this handler does not implement
custom retry logic).
"""
import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SES_FROM_EMAIL     = os.environ["SES_FROM_EMAIL"]
NOTIFICATION_EMAIL = os.environ["NOTIFICATION_EMAIL"]

ses = boto3.client("ses")


def lambda_handler(event, context):
    logger.info("Received %d SNS record(s)", len(event.get("Records", [])))
    for record in event.get("Records", []):
        sns_message = record.get("Sns", {})
        raw_body    = sns_message.get("Message", "{}")
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            logger.error("Could not parse SNS message as JSON: %s", raw_body)
            continue
        _send_email(payload, sns_message.get("Subject"))
    return {"statusCode": 200}


def _send_email(payload: dict, sns_subject: str | None) -> None:
    stage     = payload.get("stage", "unknown")
    status    = payload.get("status", "UNKNOWN")
    detail    = payload.get("detail", "(no detail provided)")
    timestamp = payload.get("timestamp", "")

    subject = sns_subject or f"IATA Pipeline — {stage} — {status}"
    body_text = (
        f"Pipeline stage : {stage}\n"
        f"Status         : {status}\n"
        f"Timestamp      : {timestamp}\n"
        f"\n"
        f"Detail:\n{detail}\n"
    )

    try:
        ses.send_email(
            Source=SES_FROM_EMAIL,
            Destination={"ToAddresses": [NOTIFICATION_EMAIL]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body_text, "Charset": "UTF-8"}},
            },
        )
        logger.info("Sent email: %s", subject)
    except Exception as exc:
        # Common cause: SES sandbox mode, one or both addresses not
        # yet verified. Logged, not raised — a notification failure
        # should never look like a pipeline failure in CloudWatch.
        logger.error("SES send failed (check both addresses are SES-verified): %s", exc)