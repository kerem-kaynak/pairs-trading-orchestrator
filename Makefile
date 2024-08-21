# Makefile for running Airflow locally

# Set Airflow home directory
AIRFLOW_HOME := $(HOME)/pairs-trading-orchestrator

# Default target
.PHONY: run

run:
	@echo "Starting Airflow..."
	@AIRFLOW_HOME=$(AIRFLOW_HOME) \
	AIRFLOW__CORE__DAGS_FOLDER=$(AIRFLOW_HOME)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	airflow standalone