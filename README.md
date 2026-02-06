🛠️ Retail Orders ETL Project
📌 Project Overview

This project is an end-to-end ETL pipeline for retail store orders. Raw order data is extracted from CSV files, transformed for data quality and enrichment, and loaded into a PostgreSQL database. The project demonstrates production-style ETL design, incremental loading, logging, and secure secret management using HashiCorp Vault.

It is part of a Data Engineering learning path and is fully modular, testable, and production-ready.

🔧 ETL Pipeline Workflow
Visual Flow
CSV Files (data_sources/) 
        │
        ▼
Extract (Python)
        │
        ▼
Transform (Pandas)
- Cleaning
- Enriching
        │
        ▼
Load (PostgreSQL via SQLAlchemy)
        │
        ▼
Logging & Testing (Pytest, logs/etl_log.txt)

Steps

Extract

Reads raw CSV files from data_sources/.

Saves raw files to raw_data/ for traceability.

Checks for required columns and missing values.

Transform

Cleans invalid or missing data.

Creates derived fields (tax, amount_category).

Performs data quality checks:

Nulls, negative amounts, duplicates.

Load

Inserts new records into PostgreSQL using incremental load.

Prevents duplicates and ensures idempotent runs.

Supports parallel insertion (CPU-optimized) for large datasets.

Logging

Logs every stage to logs/etl_log.txt with timestamps, levels, and messages.

INFO: Step completions, file paths, row counts.

WARNING: Data issues (nulls, negatives, duplicates).

ERROR: Fatal failures (missing files, DB issues).

Testing

Automated tests with Pytest:

CSV extraction check

Transformed column validation

No negative amounts

📁 Project Structure
etl_project/
│
├── extract/                 
│   └── extract_orders.py     # Extract raw CSV data
│
├── transform/               
│   └── transform_orders.py   # Data cleaning & transformation
│
├── load/                    
│   └── load_to_postgres.py   # Load data into PostgreSQL
│
├── tests/                   
│   └── test_etl_pipeline.py  # Automated ETL tests
│
├── data_sources/            # Source CSVs (empty on GitHub)
├── raw_data/                # Extracted raw files
├── transform/               # Transformed CSV & Parquet files
├── logs/                    # ETL execution logs
│   └── etl_log.txt
├── run_pipeline.py          # ETL orchestration
├── logger_config.py         # Centralized logger setup
├── vault_client.py          # Vault integration for secrets
├── requirements.txt         # Python dependencies
├── README.md
└── .gitignore


⚙️ Technologies Used

Python – Core ETL logic

Pandas – Transformation and validation

PostgreSQL – Target database

SQLAlchemy – DB connectivity & ORM

Pytest – Automated testing

Docker – Optional containerized DB setup

HashiCorp Vault – Secrets management

🔐 Secrets Management with Vault

Purpose: Keep credentials (username, password, host, port, db) out of code.

Development Setup
docker run --cap-add=IPC_LOCK \
  -e VAULT_DEV_ROOT_TOKEN_ID=root \
  -e VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200 \
  -p 8200:8200 \
  --name vault-dev \
  hashicorp/vault:latest

docker start vault-dev

# Enable KV v2 secrets engine
docker exec -it vault-dev sh -c "export VAULT_ADDR='http://127.0.0.1:8200' && export VAULT_TOKEN='root' && vault secrets enable -path=etl kv-v2"

# Store PostgreSQL credentials
docker exec -it vault-dev sh -c "
export VAULT_ADDR='http://127.0.0.1:8200' &&
export VAULT_TOKEN='root' &&
vault kv put etl/postgres username='postgres' password='1234' host='localhost' dbname='data_engineer_project' port='5432'
"

# Retrieve credentials
docker exec -it vault-dev sh -c "export VAULT_ADDR='http://127.0.0.1:8200' && export VAULT_TOKEN='root' && vault kv get etl/postgres"

Usage in Scripts
from vault_client import get_secret

secrets = get_secret("etl/postgres")
db_url = (
    f"postgresql://{secrets['username']}:{secrets['password']}"
    f"@{secrets['host']}:{secrets['port']}/{secrets['dbname']}"
)


No credentials are stored in code or Git.

🏃 Running the Pipeline
Manual Execution
python run_pipeline.py

Automated Scheduling

Linux / WSL (cron):

crontab -e
0 10 * * * /usr/bin/python3 /home/user/etl_project/run_pipeline.py >> /home/user/etl_project/logs/etl_log.txt 2>&1


Windows Task Scheduler:

Program: Python executable path

Arguments: run_pipeline.py

Start in: etl_project

Logs written to logs/etl_log.txt

✅ Testing

Run automated tests:

pytest tests/


Tests include:

Extracted CSV file exists

Required transformed columns present

No negative transaction amounts

✨ Features & Best Practices

Modular ETL (extract, transform, load)

Incremental & idempotent data loading

CPU-optimized loading for large datasets

Robust logging and monitoring

Secrets securely stored in Vault

Automated tests for data quality

Resume-ready project structure

Cron / Task Scheduler support

💡 Future Improvements

Connect to live API instead of CSV files

Use Apache Airflow or Prefect for orchestration

Add dynamic configuration and dashboards

Implement parallelized transformations for larger datasets

🎯 Project Goals & Impact

This project shows:

Real-world ETL pipeline design

Data engineering best practices

Logging, monitoring, and error handling

Secrets management in development & CI/CD

Skills applicable to Data Engineer / Analytics Engineer roles

⭐ Notes

Empty directories preserved via .gitkeep

.gitignore excludes logs, virtual environments, cache files, and secrets

Designed for both learning and portfolio demonstration