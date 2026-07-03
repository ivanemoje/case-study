SHELL := /bin/bash
.DEFAULT_GOAL := help
REGION := eu-central-1
TF_DIR := terraform/environments/dev

.PHONY: env
env: ## Print Terraform outputs as shell exports. Usage: source <(make env)
	@cd $(TF_DIR) && \
	echo "export BUCKET_NAME=$$(terraform output -raw data_bucket_name)" && \
	echo "export ATHENA_RESULTS_BUCKET=$$(terraform output -raw athena_results_bucket_name)" && \
	echo "export GLUE_DATABASE=$$(terraform output -raw glue_database)" && \
	echo "export ZIP_TO_BRONZE_JOB=$$(terraform output -raw zip_to_bronze_job_name)" && \
	echo "export BRONZE_TO_SILVER_JOB=$$(terraform output -raw bronze_to_silver_job_name)" && \
	echo "export ATHENA_WORKGROUP=$$(terraform output -raw athena_workgroup)" && \
	echo "export LAMBDA_ACQUIRE=$$(terraform output -raw lambda_acquire_name)" && \
	echo "export LAMBDA_TRIGGER_GLUE=$$(terraform output -raw lambda_trigger_glue_zip_name)" && \
	echo "export PROCESSED_FILES_TABLE=$$(terraform output -raw processed_files_table_name)" && \
	echo "export AWS_DEFAULT_REGION=$(REGION)"

.PHONY: init fmt validate plan apply destroy
init: ## Initialize Terraform
	cd $(TF_DIR) && terraform init

fmt: ## Format Terraform files
	terraform fmt -recursive terraform

validate: ## Validate Terraform
	cd $(TF_DIR) && terraform validate

plan: ## Build a Terraform plan
	cd $(TF_DIR) && terraform plan

apply: ## Apply Terraform
	cd $(TF_DIR) && terraform apply

destroy: ## Destroy Terraform-managed resources
	cd $(TF_DIR) && terraform destroy

.PHONY: iam-create-policy iam-attach iam-update-policy
iam-create-policy: ## Create the deploy IAM policy
	aws iam create-policy \
		--policy-name iata-case-study-deploy \
		--policy-document file://terraform/iam_deploy_policy.json \
		--region $(REGION)

iam-attach: ## Attach deploy policy. Usage: make iam-attach IAM_USER=training
	@test -n "$(IAM_USER)" || (echo "Usage: make iam-attach IAM_USER=your-username" && exit 1)
	@ACCOUNT=$$(aws sts get-caller-identity --query Account --output text); \
	aws iam attach-user-policy \
		--user-name $(IAM_USER) \
		--policy-arn arn:aws:iam::$$ACCOUNT:policy/iata-case-study-deploy

iam-update-policy: ## Publish the current deploy policy as the default version
	@ACCOUNT=$$(aws sts get-caller-identity --query Account --output text); \
	POLICY_ARN=arn:aws:iam::$$ACCOUNT:policy/iata-case-study-deploy; \
	aws iam create-policy-version --policy-arn $$POLICY_ARN \
		--policy-document file://terraform/iam_deploy_policy.json --set-as-default; \
	VERSIONS=$$(aws iam list-policy-versions --policy-arn $$POLICY_ARN \
		--query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text); \
	for v in $$VERSIONS; do \
		aws iam delete-policy-version --policy-arn $$POLICY_ARN --version-id $$v 2>/dev/null || true; \
	done

.PHONY: acquire drop-file
acquire: ## Invoke the acquire Lambda
	@test -n "$$LAMBDA_ACQUIRE" || (echo "Run: source <(make env)" && exit 1)
	aws lambda invoke \
		--function-name $$LAMBDA_ACQUIRE \
		--region $(REGION) \
		--payload '{}' \
		--cli-binary-format raw-in-base64-out \
		/tmp/acquire.json
	@echo && cat /tmp/acquire.json && echo

drop-file: ## Upload a ZIP to landing/. Usage: make drop-file FILE=./sales.zip
	@test -n "$$BUCKET_NAME" || (echo "Run: source <(make env)" && exit 1)
	@test -n "$(FILE)" || (echo "Usage: make drop-file FILE=./path/to/file.zip" && exit 1)
	@test -f "$(FILE)" || (echo "File not found: $(FILE)" && exit 1)
	aws s3 cp "$(FILE)" \
		s3://$$BUCKET_NAME/landing/ingest_date=$$(date -u +%F)/$$(basename "$(FILE)") \
		--region $(REGION)

.PHONY: watch-bronze watch-silver logs-acquire logs-trigger
watch-bronze: ## Watch the newest ZIP-to-bronze run
	@test -n "$$ZIP_TO_BRONZE_JOB" || (echo "Run: source <(make env)" && exit 1)
	@RUN_ID=$$(aws glue get-job-runs --job-name $$ZIP_TO_BRONZE_JOB --region $(REGION) \
		--query 'JobRuns[0].Id' --output text); \
	watch -n 15 "aws glue get-job-run --job-name $$ZIP_TO_BRONZE_JOB --run-id $$RUN_ID \
		--region $(REGION) --query 'JobRun.{State:JobRunState,Error:ErrorMessage}'"

watch-silver: ## Watch the newest bronze-to-silver run
	@test -n "$$BRONZE_TO_SILVER_JOB" || (echo "Run: source <(make env)" && exit 1)
	@RUN_ID=$$(aws glue get-job-runs --job-name $$BRONZE_TO_SILVER_JOB --region $(REGION) \
		--query 'JobRuns[0].Id' --output text); \
	watch -n 15 "aws glue get-job-run --job-name $$BRONZE_TO_SILVER_JOB --run-id $$RUN_ID \
		--region $(REGION) --query 'JobRun.{State:JobRunState,Error:ErrorMessage}'"

logs-acquire: ## Tail acquire Lambda logs
	@test -n "$$LAMBDA_ACQUIRE" || (echo "Run: source <(make env)" && exit 1)
	aws logs tail /aws/lambda/$$LAMBDA_ACQUIRE --follow --region $(REGION)

logs-trigger: ## Tail landing-trigger Lambda logs
	@test -n "$$LAMBDA_TRIGGER_GLUE" || (echo "Run: source <(make env)" && exit 1)
	aws logs tail /aws/lambda/$$LAMBDA_TRIGGER_GLUE --follow --region $(REGION)

.PHONY: landing-list archive-list staging-list ledger-list ledger-clear
landing-list: ## List transient landing files
	aws s3 ls s3://$$BUCKET_NAME/landing/ --recursive --region $(REGION)

archive-list: ## List archived source ZIPs
	aws s3 ls s3://$$BUCKET_NAME/archive/ --recursive --region $(REGION)

staging-list: ## List temporary extracted CSVs; normally empty
	aws s3 ls s3://$$BUCKET_NAME/staging/ --recursive --region $(REGION)

ledger-list: ## Show processed-file ledger entries
	@test -n "$$PROCESSED_FILES_TABLE" || (echo "Run: source <(make env)" && exit 1)
	aws dynamodb scan \
		--table-name $$PROCESSED_FILES_TABLE \
		--region $(REGION) \
		--query 'Items[].{checksum:checksum_sha256.S,status:status.S,landing:landing_key.S,archive:archive_key.S,rows:bronze_row_count.N}' \
		--output table

ledger-clear: ## Delete all ledger entries
	@test -n "$$PROCESSED_FILES_TABLE" || (echo "Run: source <(make env)" && exit 1)
	@read -p "Delete every ledger entry? [y/N] " confirm; \
	if [ "$$confirm" = "y" ]; then \
		aws dynamodb scan --table-name $$PROCESSED_FILES_TABLE --region $(REGION) \
			--query 'Items[].checksum_sha256.S' --output text | tr '\t' '\n' | \
		while read -r checksum; do \
			[ -n "$$checksum" ] && aws dynamodb delete-item \
				--table-name $$PROCESSED_FILES_TABLE \
				--key "{\"checksum_sha256\":{\"S\":\"$$checksum\"}}" \
				--region $(REGION); \
		done; \
	fi

.PHONY: reset-bronze reset-silver reset-all
reset-bronze: ## Drop bronze and remove its S3 data
	aws glue delete-table --database-name $$GLUE_DATABASE --name sales_bronze --region $(REGION) || true
	aws s3 rm s3://$$BUCKET_NAME/bronze/ --recursive --region $(REGION) || true

reset-silver: ## Drop silver/quarantine and remove their S3 data
	aws glue delete-table --database-name $$GLUE_DATABASE --name sales_silver --region $(REGION) || true
	aws glue delete-table --database-name $$GLUE_DATABASE --name sales_quarantine --region $(REGION) || true
	aws s3 rm s3://$$BUCKET_NAME/silver/ --recursive --region $(REGION) || true
	aws s3 rm s3://$$BUCKET_NAME/quarantine/ --recursive --region $(REGION) || true

reset-all: reset-silver reset-bronze ## Reset all data tables

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
