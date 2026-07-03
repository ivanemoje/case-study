# IATA Case Study — Makefile
# Run `make help` for a full list of targets.

SHELL := /bin/bash
.DEFAULT_GOAL := help
REGION := eu-central-2
TF_DIR := terraform/environments/dev

# ─────────────────────────────────────────────────────────────
# ENV — usage: source <(make env)
# ─────────────────────────────────────────────────────────────

.PHONY: env
env: ## Print export statements for Terraform outputs. Usage: source <(make env)
	@cd $(TF_DIR) && \
	echo "export BUCKET_NAME=$$(terraform output -raw data_bucket_name)" && \
	echo "export ATHENA_RESULTS_BUCKET=$$(terraform output -raw athena_results_bucket_name)" && \
	echo "export GLUE_DATABASE=$$(terraform output -raw glue_database)" && \
	echo "export ZIP_TO_BRONZE_JOB=$$(terraform output -raw zip_to_bronze_job_name)" && \
	echo "export BRONZE_TO_SILVER_JOB=$$(terraform output -raw bronze_to_silver_job_name)" && \
	echo "export ATHENA_WORKGROUP=$$(terraform output -raw athena_workgroup)" && \
	echo "export LAMBDA_ACQUIRE=$$(terraform output -raw lambda_acquire_name)" && \
	echo "export LAMBDA_TRIGGER_GLUE=$$(terraform output -raw lambda_trigger_glue_zip_name)" && \
	echo "export LAMBDA_SES_SENDER=$$(terraform output -raw lambda_ses_sender_name)" && \
	echo "export PROCESSED_FILES_TABLE=$$(terraform output -raw processed_files_table_name)" && \
	echo "export AWS_DEFAULT_REGION=$(REGION)"

# ─────────────────────────────────────────────────────────────
# TERRAFORM
# ─────────────────────────────────────────────────────────────

.PHONY: init plan apply destroy validate
init: ## terraform init
	cd $(TF_DIR) && terraform init

validate: ## terraform validate
	cd $(TF_DIR) && terraform validate

plan: ## terraform plan
	cd $(TF_DIR) && terraform plan

apply: ## terraform apply (asks for confirmation)
	cd $(TF_DIR) && terraform apply

destroy: ## terraform destroy (asks for confirmation)
	cd $(TF_DIR) && terraform destroy

# ─────────────────────────────────────────────────────────────
# IAM — your own deploy permissions, separate from the Terraform-
# managed roles the pipeline uses internally.
# ─────────────────────────────────────────────────────────────

.PHONY: iam-create-policy iam-attach iam-update-policy
iam-create-policy: ## Create the deploy IAM policy (one-time)
	aws iam create-policy \
		--policy-name iata-case-study-deploy \
		--policy-document file://terraform/iam_deploy_policy.json \
		--region $(REGION)

iam-attach: ## Attach the deploy policy to an IAM user. Usage: make iam-attach IAM_USER=training
	@test -n "$(IAM_USER)" || (echo "Usage: make iam-attach IAM_USER=your-username" && exit 1)
	@ACCOUNT=$$(aws sts get-caller-identity --query Account --output text); \
	aws iam attach-user-policy \
		--user-name $(IAM_USER) \
		--policy-arn arn:aws:iam::$$ACCOUNT:policy/iata-case-study-deploy

iam-update-policy: ## Push a changed iam_deploy_policy.json as a new version (handles the 5-version IAM limit)
	@ACCOUNT=$$(aws sts get-caller-identity --query Account --output text); \
	POLICY_ARN=arn:aws:iam::$$ACCOUNT:policy/iata-case-study-deploy; \
	aws iam create-policy-version --policy-arn $$POLICY_ARN \
		--policy-document file://terraform/iam_deploy_policy.json --set-as-default; \
	VERSIONS=$$(aws iam list-policy-versions --policy-arn $$POLICY_ARN \
		--query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text); \
	for v in $$VERSIONS; do \
		aws iam delete-policy-version --policy-arn $$POLICY_ARN --version-id $$v 2>/dev/null || true; \
	done

# ─────────────────────────────────────────────────────────────
# RUN PIPELINE
# Requires: source <(make env)  run first in your shell
# ─────────────────────────────────────────────────────────────

.PHONY: acquire drop-file
acquire: ## Manually invoke the acquire Lambda (download zip → upload to raw/)
	@test -n "$$LAMBDA_ACQUIRE" || (echo "Run: source <(make env)" && exit 1)
	aws lambda invoke \
		--function-name $$LAMBDA_ACQUIRE \
		--region $(REGION) \
		--payload '{}' \
		--cli-binary-format raw-in-base64-out \
		/tmp/acquire.json
	@echo ""
	@cat /tmp/acquire.json
	@echo ""

drop-file: ## Manually upload a local zip to raw/ to test the EventBridge trigger. Usage: make drop-file FILE=./my-data.zip
	@test -n "$$BUCKET_NAME" || (echo "Run: source <(make env)" && exit 1)
	@test -n "$(FILE)" || (echo "Usage: make drop-file FILE=./path/to/file.zip" && exit 1)
	@test -f "$(FILE)" || (echo "File not found: $(FILE)" && exit 1)
	aws s3 cp "$(FILE)" s3://$$BUCKET_NAME/raw/$$(basename $(FILE)) --region $(REGION)
	@echo ""
	@echo "Uploaded. EventBridge should fire trigger_glue_zip within a few seconds."
	@echo "Watch with: make logs-trigger"

# ─────────────────────────────────────────────────────────────
# MONITOR
# ─────────────────────────────────────────────────────────────

.PHONY: watch-bronze watch-silver logs-acquire logs-trigger logs-ses
watch-bronze: ## Poll the most recent zip_to_bronze job run until it finishes
	@test -n "$$ZIP_TO_BRONZE_JOB" || (echo "Run: source <(make env)" && exit 1)
	@RUN_ID=$$(aws glue get-job-runs --job-name $$ZIP_TO_BRONZE_JOB --region $(REGION) \
		--query 'JobRuns[0].Id' --output text); \
	watch -n 15 "aws glue get-job-run --job-name $$ZIP_TO_BRONZE_JOB --run-id $$RUN_ID \
		--region $(REGION) --query 'JobRun.{State:JobRunState,Error:ErrorMessage}'"

watch-silver: ## Poll the most recent bronze_to_silver job run until it finishes
	@test -n "$$BRONZE_TO_SILVER_JOB" || (echo "Run: source <(make env)" && exit 1)
	@RUN_ID=$$(aws glue get-job-runs --job-name $$BRONZE_TO_SILVER_JOB --region $(REGION) \
		--query 'JobRuns[0].Id' --output text); \
	watch -n 15 "aws glue get-job-run --job-name $$BRONZE_TO_SILVER_JOB --run-id $$RUN_ID \
		--region $(REGION) --query 'JobRun.{State:JobRunState,Error:ErrorMessage}'"

logs-acquire: ## Tail acquire Lambda logs
	@test -n "$$LAMBDA_ACQUIRE" || (echo "Run: source <(make env)" && exit 1)
	aws logs tail /aws/lambda/$$LAMBDA_ACQUIRE --follow --region $(REGION)

logs-trigger: ## Tail trigger_glue_zip Lambda logs (fires on every raw/ upload)
	@test -n "$$LAMBDA_TRIGGER_GLUE" || (echo "Run: source <(make env)" && exit 1)
	aws logs tail /aws/lambda/$$LAMBDA_TRIGGER_GLUE --follow --region $(REGION)

logs-ses: ## Tail ses_sender Lambda logs (shows notification send attempts)
	@test -n "$$LAMBDA_SES_SENDER" || (echo "Run: source <(make env)" && exit 1)
	aws logs tail /aws/lambda/$$LAMBDA_SES_SENDER --follow --region $(REGION)

# ─────────────────────────────────────────────────────────────
# LEDGER — inspect the checksum dedup table
# ─────────────────────────────────────────────────────────────

.PHONY: ledger-list ledger-clear
ledger-list: ## List all entries in the processed-files DynamoDB ledger
	@test -n "$$PROCESSED_FILES_TABLE" || (echo "Run: source <(make env)" && exit 1)
	aws dynamodb scan \
		--table-name $$PROCESSED_FILES_TABLE \
		--region $(REGION) \
		--query 'Items[].{checksum:checksum_sha256.S,raw_key:raw_key.S,status:status.S}' \
		--output table

ledger-clear: ## Delete all entries from the ledger (use to force re-processing during testing)
	@test -n "$$PROCESSED_FILES_TABLE" || (echo "Run: source <(make env)" && exit 1)
	@echo "This deletes every ledger entry. Re-running acquire will re-upload and re-process everything."
	@read -p "Continue? [y/N] " confirm; \
	if [ "$$confirm" = "y" ]; then \
		aws dynamodb scan --table-name $$PROCESSED_FILES_TABLE --region $(REGION) \
			--query 'Items[].checksum_sha256.S' --output text | tr '\t' '\n' | \
		while read -r checksum; do \
			[ -n "$$checksum" ] && aws dynamodb delete-item \
				--table-name $$PROCESSED_FILES_TABLE \
				--key "{\"checksum_sha256\":{\"S\":\"$$checksum\"}}" \
				--region $(REGION); \
		done; \
		echo "Ledger cleared."; \
	fi

# ─────────────────────────────────────────────────────────────
# RESET — drop and recreate tables (use after schema changes)
# ─────────────────────────────────────────────────────────────

.PHONY: reset-bronze reset-silver reset-all
reset-bronze: ## Drop bronze table + S3 data
	@test -n "$$BUCKET_NAME" || (echo "Run: source <(make env)" && exit 1)
	aws glue delete-table --database-name $$GLUE_DATABASE --name sales_bronze --region $(REGION) || true
	aws s3 rm s3://$$BUCKET_NAME/bronze/ --recursive --region $(REGION) || true
	@echo "Bronze reset."

reset-silver: ## Drop silver + quarantine tables + S3 data
	@test -n "$$BUCKET_NAME" || (echo "Run: source <(make env)" && exit 1)
	aws glue delete-table --database-name $$GLUE_DATABASE --name sales_silver --region $(REGION) || true
	aws glue delete-table --database-name $$GLUE_DATABASE --name sales_quarantine --region $(REGION) || true
	aws s3 rm s3://$$BUCKET_NAME/silver/ --recursive --region $(REGION) || true
	aws s3 rm s3://$$BUCKET_NAME/quarantine/ --recursive --region $(REGION) || true
	@echo "Silver + quarantine reset."

reset-all: reset-silver reset-bronze ## Drop everything (bronze, silver, quarantine)

# ─────────────────────────────────────────────────────────────
# HELP
# ─────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@echo "IATA Case Study — available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Typical first-time flow:"
	@echo "  1. make iam-create-policy"
	@echo "  2. make iam-attach IAM_USER=your-username"
	@echo "  3. make init"
	@echo "  4. make plan"
	@echo "  5. make apply"
	@echo "  6. Check your email — verify both SES addresses (links AWS sent you)"
	@echo "  7. source <(make env)"
	@echo "  8. make acquire        — downloads zip, uploads to raw/, auto-triggers the rest"
	@echo "  9. make watch-bronze   — wait for zip_to_bronze to finish"
	@echo "  10. make watch-silver  — wait for bronze_to_silver (auto-triggered) to finish"
	@echo ""
	@echo "To test manual file drop instead of the Lambda:"
	@echo "  make drop-file FILE=./some-data.zip"
