# IATA Case Study — Modular Event-Driven Pipeline

**Stack:** Lambda · EventBridge · S3 · Glue (Spark + Iceberg) · DynamoDB · SNS · SES · Athena · Terraform (modular)
**Region:** eu-central-2 (Zurich)

---

## Architecture

```
Lambda: acquire
  • Downloads zip from SOURCE_URL (stays compressed)
  • SHA256 checksum → checks DynamoDB ledger → skips if already processed
  • Uploads zip to raw/sales_<timestamp>.zip
  • Publishes result to SNS (acquire topic) → ses_sender Lambda emails you
        │
        │  S3 "Object Created" event on raw/*.zip
        ▼
EventBridge rule (zip-landed)
  • Fires on ANY upload to raw/ — the acquire Lambda's upload AND
    a manual `aws s3 cp` / console drop produce identical events
        │
        ▼
Lambda: trigger_glue_zip
  • Pure bridge — extracts the S3 key, calls glue:StartJobRun
        │
        ▼
Glue Job: zip_to_bronze
  • Downloads the zip to the Glue driver, extracts the CSV (stdlib zipfile)
  • Re-checks the checksum ledger (defense in depth vs manual drops)
  • Uploads extracted CSV to intermediate/ (uncompressed)
  • Spark reads intermediate/ CSV in a fully distributed read — this is
    the step that scales to large files
  • Appends to bronze Iceberg table (faithful raw copy, zero rows dropped)
  • Marks ledger entry "completed"
  • Publishes SUCCESS/FAILURE to SNS (landing topic)
        │
        │  Native Glue trigger: zip_to_bronze SUCCEEDED →
        ▼
Glue Job: bronze_to_silver  (auto-triggered, no manual step)
  • Quarantines rows with missing order_id (fatal — can't dedupe/MERGE)
  • Defaults missing region to 'UNKNOWN', flags region_is_synthetic
  • Deduplicates on order_id (latest _ingested_at wins)
  • Casts types, MERGE INTO silver (idempotent)
  • Publishes SUCCESS/FAILURE to SNS (silver topic)
        │
        ▼
Amazon Athena
  • Workgroup points at a SEPARATE results bucket (not the data lake)
  • That bucket has a 3-day lifecycle policy — results auto-delete
```

**Notification fan-out:** all three SNS topics (acquire, landing, silver) are subscribed by a single `ses_sender` Lambda, which formats and sends one email per event via SES. One place to change the email template; Glue jobs only need `sns:Publish`, not SES permissions.

**Checksum dedup:** a DynamoDB table (`processed_files_table`) keyed on SHA256 of the zip's bytes — not filename — means the same content under a different name is still recognised as a duplicate, while a genuinely new file reusing an old filename is correctly treated as new data.

---

## Repository Tree

```
iata-case-study/
├── Makefile
├── README.md
├── lambdas/
│   ├── acquire/
│   │   └── handler.py
│   ├── trigger_glue_zip/
│   │   └── handler.py
│   └── ses_sender/
│       └── handler.py
├── glue_jobs/
│   ├── zip_to_bronze.py
│   └── bronze_to_silver.py
└── terraform/
    ├── iam_deploy_policy.json          ← attach to YOUR IAM user (deploy permissions)
    ├── modules/
    │   ├── s3/                         ← data lake bucket, athena results bucket, ledger table
    │   │   ├── main.tf
    │   │   ├── dynamodb.tf
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   ├── iam/                        ← all pipeline roles + policies (Lambda, Glue execution)
    │   │   ├── main.tf
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   ├── lambda/                     ← acquire, trigger_glue_zip, ses_sender functions
    │   │   ├── main.tf
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   ├── glue/                       ← catalog database, 2 jobs, native job-chain trigger
    │   │   ├── main.tf
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   ├── eventbridge/                ← S3 event rule → trigger_glue_zip
    │   │   ├── main.tf
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   ├── athena/                     ← workgroup pointed at results bucket
    │   │   ├── main.tf
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   └── ses/                        ← email identities + SNS topics
    │       ├── main.tf
    │       ├── variables.tf
    │       └── outputs.tf
    └── environments/
        └── dev/
            ├── main.tf                 ← wires all modules together
            ├── variables.tf
            ├── outputs.tf
            └── terraform.tfvars        ← the ONLY file you edit per environment
```

---

## IAM Setup — Your Own Deploy Permissions

This is the policy YOU need attached to your IAM user/role to run `terraform apply`. It is separate from the pipeline's internal roles (Lambda execution roles, Glue role), which Terraform creates and manages itself.

```bash
make iam-create-policy
make iam-attach IAM_USER=training
```

If you ever hit an `AccessDenied` error during `terraform apply` (the AWS provider calls some read-back API your policy doesn't yet cover), edit `terraform/iam_deploy_policy.json` and run:

```bash
make iam-update-policy
```

This handles IAM's policy version churn for you (max 5 versions; creates a new default, deletes old non-default versions).

---

## Prerequisites

| Tool | Notes |
|---|---|
| Terraform >= 1.5 | |
| AWS CLI v2 | `aws configure --profile iata-case-study` |
| make | Pre-installed on macOS/Linux |
| Two email addresses | Can be the same address — used for SES sender + recipient |

```bash
export AWS_PROFILE=iata-case-study
export AWS_DEFAULT_REGION=eu-central-2
aws sts get-caller-identity   # verify it works
```

---

## Configure

Edit `terraform/environments/dev/terraform.tfvars` — this is the only file you need to touch per environment:

```hcl
data_bucket_name            = "iata-lake-yourname-202606"          # must be globally unique
athena_results_bucket_name  = "iata-athena-results-yourname-202606" # must be globally unique
notification_email          = "you@example.com"
ses_from_email               = "you@example.com"
```

---

## Deploy — Step by Step

```bash
make iam-create-policy
make iam-attach IAM_USER=training

make init
make validate
make plan      # review — expect ~45 resources across all modules
make apply     # type "yes"
```

**Critical — SES sandbox verification:** AWS accounts start in SES sandbox mode, meaning you can only send to/from *verified* email addresses. After `apply`, AWS sends a verification email to both `ses_from_email` and `notification_email` (the same email if you used the same address for both — you'll get two separate verification links, click both). **Notifications will silently fail to send until you click these links.** The Terraform output `ses_verification_reminder` repeats this for you:

```bash
cd terraform/environments/dev && terraform output ses_verification_reminder
```

---

## Run the Pipeline

```bash
source <(make env)    # loads all Terraform outputs into your shell
```

### Option A — trigger via Lambda (the normal path)

```bash
make acquire
```

This downloads the source zip, checksums it, uploads to `raw/`, and publishes to SNS. The EventBridge rule fires automatically, which starts `zip_to_bronze`, which on success automatically triggers `bronze_to_silver` via the native Glue job trigger — **no manual step needed between bronze and silver.**

```bash
make watch-bronze    # blocks until zip_to_bronze finishes
make watch-silver    # blocks until bronze_to_silver finishes (auto-started)
```

### Option B — manual file drop (tests the EventBridge path independently of the Lambda)

```bash
make drop-file FILE=./path/to/some-sales-data.zip
```

Uploads directly to `raw/` via `aws s3 cp`. This produces the exact same S3 "Object Created" event as the acquire Lambda's upload, so EventBridge fires identically — this is what proves the pipeline is event-driven, not just Lambda-driven.

```bash
make logs-trigger    # watch trigger_glue_zip pick up the file
make watch-bronze
make watch-silver
```

### Testing the checksum dedup

```bash
make acquire          # first run — uploads and processes
make acquire          # second run — same SOURCE_URL, same content, same checksum
                       # → SKIPPED, no duplicate upload, no duplicate bronze write
make ledger-list       # see the ledger entry
```

To force re-processing during testing:

```bash
make ledger-clear
```

---

## Monitor

```bash
make logs-acquire     # acquire Lambda logs
make logs-trigger     # trigger_glue_zip Lambda logs
make logs-ses         # ses_sender Lambda logs — confirms whether SES sends succeeded/failed
make watch-bronze      # poll zip_to_bronze job status
make watch-silver      # poll bronze_to_silver job status
make ledger-list       # see every processed file + checksum + status
```

---

## Query with Athena

Console → Athena → workgroup `iata-case-study-workgroup` → database `iata_lake`.

```sql
-- Validation
SELECT 'bronze' AS layer, COUNT(*) AS rows FROM iata_lake.sales_bronze
UNION ALL
SELECT 'silver', COUNT(*) FROM iata_lake.sales_silver
UNION ALL
SELECT 'quarantine', COUNT(*) FROM iata_lake.sales_quarantine;

-- Confirm quarantine caught missing order_id
SELECT _quarantine_reason, COUNT(*) FROM iata_lake.sales_quarantine GROUP BY 1;

-- Confirm region defaulting worked, and silver has zero NULL order_id
SELECT COUNT(*) FILTER (WHERE region_is_synthetic) AS synthetic_region_rows,
       COUNT(*) FILTER (WHERE order_id IS NULL)     AS null_order_id_rows  -- must be 0
FROM iata_lake.sales_silver;

-- Business query — revenue by region
SELECT region, COUNT(*) AS orders, ROUND(SUM(total_revenue),2) AS revenue,
       ROUND(SUM(total_profit),2) AS profit
FROM iata_lake.sales_silver
GROUP BY region
ORDER BY profit DESC;
```

Query results land in the **separate** `athena-results` bucket and auto-delete after 3 days (configurable via `athena_results_expiry_days` in `terraform.tfvars`) — they never touch the data lake bucket.

---

## Teardown

```bash
make destroy
```

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
POLICY_ARN=arn:aws:iam::${ACCOUNT}:policy/iata-case-study-deploy
aws iam detach-user-policy --user-name training --policy-arn $POLICY_ARN
aws iam list-policy-versions --policy-arn $POLICY_ARN   # delete non-default versions first
aws iam delete-policy --policy-arn $POLICY_ARN
```

---

## Troubleshooting

**`terraform apply` fails with AccessDenied** → `make iam-update-policy` after adding the missing action to `terraform/iam_deploy_policy.json`.

**No emails arriving** → check `make logs-ses`. Almost always: one or both SES addresses not yet verified. Check inbox, click the link, re-run `make acquire`.

**`zip_to_bronze` never starts after uploading to raw/** → confirm the EventBridge rule is enabled:
```bash
aws events describe-rule --name iata-case-study-zip-landed --region eu-central-2 --query State
```
Confirm S3 → EventBridge notifications are on for the bucket (the `eventbridge` module sets this, but verify):
```bash
aws s3api get-bucket-notification-configuration --bucket $BUCKET_NAME --region eu-central-2
```

**Glue job fails** → check error logs:
```bash
aws logs filter-log-events --log-group-name /aws-glue/jobs/error --filter-pattern "ERROR" --region eu-central-2 --limit 20
```

**Want to re-process a file you already ran** → `make ledger-clear` then `make acquire` or `make drop-file`.

---

## Design Decisions — Debrief Prep

**Why Terraform is modularized this way:** each module maps to one AWS service domain (s3, iam, lambda, glue, eventbridge, athena, ses), not to a pipeline stage. This means adding a fourth Lambda or a third Glue job extends an existing module rather than requiring a new one — the module boundary is "what AWS API surface does this touch," which is the stable axis as the pipeline evolves.

**Why EventBridge instead of direct S3 bucket notifications:** S3's native `aws_s3_bucket_notification` only supports one configuration block per bucket in Terraform state, which gets fragile as more consumers are added. Routing through EventBridge decouples the bucket from any specific consumer and means the acquire Lambda's upload and a manual console/CLI drop produce *identical* events handled by one rule — proving the system is genuinely event-driven, not Lambda-driven with EventBridge bolted on.

**Why extraction happens in Glue, not Lambda:** Lambda has a 15-minute ceiling and fixed memory. The case study brief explicitly flags "big data" CSVs — Spark's distributed read after driver-side unzip is the part that scales; Lambda alone cannot for files beyond its memory/disk limits.

**Why SNS sits between everything and SES:** Glue jobs and the acquire Lambda only need `sns:Publish` — a single, simple permission — rather than SES send permissions scattered across three different execution roles. The email template lives in exactly one place (`ses_sender`), and adding a Slack or PagerDuty subscriber later means adding a new SNS subscription, not touching the Glue jobs.

**Why the checksum ledger is checked twice (acquire Lambda AND zip_to_bronze):** the acquire Lambda's check prevents wasted downloads/uploads in the common case. The Glue job's independent check is defense in depth — someone can drop a duplicate file via console or CLI, bypassing the Lambda check entirely, and the pipeline still correctly skips reprocessing.

**Why Athena results live in a separate bucket:** isolates an aggressive lifecycle policy (3-day expiry) from the actual data lake, and keeps the lake's EventBridge rule from ever needing to filter out Athena's own result objects.
