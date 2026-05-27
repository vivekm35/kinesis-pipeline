.PHONY: install test test-integration lint format package \
        infra-plan infra-apply infra-destroy \
        local-up local-down local-seed \
        producer-run shard-monitor dlq-replay \
        redshift-connect clean help

PYTHON      := python3
PIP         := $(PYTHON) -m pip
PYTEST      := $(PYTHON) -m pytest
TERRAFORM   := terraform
TF_DIR      := infrastructure/terraform
ENV         ?= dev

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

install: ## Install all Python dependencies
	$(PIP) install -r requirements.txt

test: ## Unit tests with coverage (no AWS needed)
	$(PYTEST) tests/ --ignore=tests/integration \
	  --cov=lambda_fn --cov=producer --cov-report=term-missing -v

test-integration: ## Integration tests (requires: make local-up)
	$(PYTEST) tests/integration/ -v -s

lint: ## flake8 + black check + isort check
	$(PYTHON) -m flake8 producer/ lambda_fn/ redshift/ scripts/ tests/ --max-line-length=100
	$(PYTHON) -m black --check --line-length=100 producer/ lambda_fn/ redshift/ scripts/ tests/
	$(PYTHON) -m isort --check-only producer/ lambda_fn/ redshift/ scripts/ tests/

format: ## Auto-format with black + isort
	$(PYTHON) -m black --line-length=100 producer/ lambda_fn/ redshift/ scripts/ tests/
	$(PYTHON) -m isort producer/ lambda_fn/ redshift/ scripts/ tests/

package: ## Build Lambda deployment zip
	rm -rf lambda_fn/package lambda.zip
	$(PIP) install -r requirements.txt --target lambda_fn/package
	cd lambda_fn/package && zip -r ../../lambda.zip . -x "*.pyc" -x "__pycache__/*"
	cd lambda_fn && zip -g ../lambda.zip handler.py idempotency.py
	zip -g lambda.zip -r lambda_fn/utils/
	@echo "Lambda package: $$(du -sh lambda.zip | cut -f1)"

infra-plan: ## Terraform plan (ENV=dev|prod)
	cd $(TF_DIR) && $(TERRAFORM) init && $(TERRAFORM) plan -var-file="envs/$(ENV).tfvars"

infra-apply: ## Terraform apply (ENV=dev|prod)
	cd $(TF_DIR) && $(TERRAFORM) init && $(TERRAFORM) apply -var-file="envs/$(ENV).tfvars"

infra-destroy: ## Terraform destroy — DESTRUCTIVE
	cd $(TF_DIR) && $(TERRAFORM) destroy -var-file="envs/$(ENV).tfvars"

local-up: ## Start LocalStack + Postgres
	docker compose -f docker/docker-compose.yml up -d
	@echo "Waiting for LocalStack..." && until curl -sf http://localhost:4566/_localstack/health > /dev/null; do sleep 2; done && echo "Ready"

local-down: ## Stop local Docker services
	docker compose -f docker/docker-compose.yml down -v

local-seed: ## Seed LocalStack with pipeline resources
	AWS_ENDPOINT_URL=http://localhost:4566 bash scripts/seed_localstack.sh

producer-run: ## Run producer against real AWS
	$(PYTHON) producer/producer.py --stream $${KINESIS_STREAM_NAME} --rate 5000 --duration 60 --threads 4

producer-local: ## Run producer against LocalStack
	AWS_ENDPOINT_URL=http://localhost:4566 $(PYTHON) producer/producer.py --stream kinesis-pipeline-dev-events --rate 100 --duration 30

shard-monitor: ## Real-time shard monitor (real AWS)
	$(PYTHON) scripts/shard_monitor.py --stream $${KINESIS_STREAM_NAME}

shard-monitor-local: ## Shard monitor against LocalStack
	$(PYTHON) scripts/shard_monitor.py --stream kinesis-pipeline-dev-events --endpoint http://localhost:4566

dlq-replay: ## DLQ replay dry-run
	$(PYTHON) scripts/dlq_replay.py --dry-run

dlq-replay-live: ## Live DLQ replay (up to 500 records)
	$(PYTHON) scripts/dlq_replay.py --max-messages 500

monitoring-setup: ## Create CloudWatch dashboard
	$(PYTHON) monitoring/cloudwatch.py --stack kinesis-pipeline-$(ENV)

redshift-connect: ## psql to Redshift
	psql -h $${REDSHIFT_HOST} -U $${REDSHIFT_USER} -d $${REDSHIFT_DB} -p $${REDSHIFT_PORT:-5439}

redshift-schema: ## Apply schema DDL
	psql -h $${REDSHIFT_HOST} -U $${REDSHIFT_USER} -d $${REDSHIFT_DB} -f redshift/schema.sql

redshift-local: ## psql to local Postgres
	psql -h localhost -U pipeline_user -d analytics -p 5432

clean: ## Remove build artifacts
	rm -rf lambda_fn/package lambda.zip .pytest_cache htmlcov coverage.xml **/__pycache__ **/*.pyc
