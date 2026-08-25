SHELL := /bin/bash

DATA_IMAGE := parking-data-pipeline-synthetic

.PHONY: help \
        setup \
        generate-data \
        build-synthetic \
        generate-data-docker \
        airflow-env \
        airflow-up \
        airflow-down \
        airflow-logs \
        tf-init \
        tf-plan \
        tf-apply \
        lint

help:
	@echo "make setup                 - create local Python environment"
	@echo "make generate-data         - generate synthetic data locally"
	@echo "make build-synthetic       - build synthetic data Docker image"
	@echo "make generate-data-docker  - generate synthetic data using Docker"
	@echo "make airflow-env           - create airflow/.env from example"
	@echo "make airflow-up            - start local Airflow"
	@echo "make airflow-down          - stop local Airflow"
	@echo "make airflow-logs          - view Airflow logs"
	@echo "make tf-init               - initialize Terraform"
	@echo "make tf-plan               - run Terraform plan"
	@echo "make tf-apply              - run Terraform apply"
	@echo "make lint                  - lint Python code"

setup:
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -r requirements-synthetic.txt

generate-data:
	cd scripts && python3 generate_synthetic_data.py

build-synthetic:
	docker build -f Dockerfile.synthetic -t $(DATA_IMAGE) .

generate-data-docker: build-synthetic
	mkdir -p data
	docker run --rm -v $(PWD)/data:/app/data $(DATA_IMAGE)

airflow-env:
	test -f airflow/.env || cp airflow/.env.example airflow/.env

airflow-up:
	cd airflow && docker compose up -d --build

airflow-down:
	cd airflow && docker compose down

airflow-logs:
	cd airflow && docker compose logs -f

tf-init:
	cd terraform && terraform init -backend-config=backend.hcl

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply

lint:
	ruff check .