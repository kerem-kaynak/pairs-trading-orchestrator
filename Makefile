# Makefile for managing Airflow locally and Cloud Composer

# Source .env file if exists
ifneq (,$(wildcard .env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

# Set Airflow home directory
AIRFLOW_HOME := $(HOME)/pairs-trading-orchestrator

# Set Cloud Composer environment details (with defaults)
COMPOSER_ENV ?= pairs-trading-orchestrator
COMPOSER_LOCATION ?= europe-west4

# Default target
.PHONY: run add-composer-env list-dags describe-env get-airflow-url trigger-dag help

# Run Airflow locally
run:
	@echo "Starting Airflow..."
	@AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(AIRFLOW_HOME)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	PYTHONPATH=$(PROJECT_ROOT):$(PYTHONPATH) \
	NO_PROXY="*" \
	GOOGLE_APPLICATION_CREDENTIALS="/Users/kerem/.config/gcloud/application_default_credentials.json" \
	airflow standalone

# Add or update Composer environment variable
add-composer-env:
	@if [ -z "$(NAME)" ] || [ -z "$(VALUE)" ]; then \
		echo "Usage: make add-composer-env NAME=variable_name VALUE=variable_value"; \
		exit 1; \
	fi
	gcloud composer environments update $(COMPOSER_ENV) \
		--location $(COMPOSER_LOCATION) \
		--update-env-variables $(NAME)=$(VALUE)

# List DAGs in Composer environment
list-dags:
	gcloud composer environments run $(COMPOSER_ENV) \
		--location $(COMPOSER_LOCATION) \
		list_dags

# View Composer environment details
describe-env:
	gcloud composer environments describe $(COMPOSER_ENV) \
		--location $(COMPOSER_LOCATION)

# View Airflow web UI URL
get-airflow-url:
	gcloud composer environments describe $(COMPOSER_ENV) \
		--location $(COMPOSER_LOCATION) \
		--format="get(config.airflowUri)"

# Trigger a specific DAG run
trigger-dag:
	@if [ -z "$(DAG_ID)" ]; then \
		echo "Usage: make trigger-dag DAG_ID=your_dag_id"; \
		exit 1; \
	fi
	gcloud composer environments run $(COMPOSER_ENV) \
		--location $(COMPOSER_LOCATION) \
		trigger_dag -- $(DAG_ID)

# Display help information
help:
	@echo "Available targets:"
	@echo "  run                  - Run Airflow locally"
	@echo "  add-composer-env     - Add/update Composer environment variable"
	@echo "  list-dags            - List DAGs in Composer environment"
	@echo "  describe-env         - View Composer environment details"
	@echo "  get-airflow-url      - Get Airflow web UI URL"
	@echo "  trigger-dag          - Trigger a specific DAG run"
	@echo "  help                 - Display this help message"
	@echo ""
	@echo "Usage examples:"
	@echo "  make run"
	@echo "  make add-composer-env NAME=MY_VAR VALUE=my_value"
	@echo "  make list-dags"
	@echo "  make describe-env"
	@echo "  make get-airflow-url"
	@echo "  make trigger-dag DAG_ID=my_dag"

# Set default target to help
.DEFAULT_GOAL := help