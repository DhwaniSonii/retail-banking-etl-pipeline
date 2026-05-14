# 🏦 Banking Data Platform — Retail Transaction Pipeline

![CI](https://github.com/your-username/banking-data-platform/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.6-orange)
![Airflow](https://img.shields.io/badge/airflow-2.7-red)
![PostgreSQL](https://img.shields.io/badge/postgresql-14-336791)
![License](https://img.shields.io/badge/license-MIT-green)

> **Live Demo:** `![Dashboard](docs/Dashboard.png)`

A production-grade data engineering project simulating a retail banking data platform. Covers the full data lifecycle: **ingestion → ETL → data modeling → quality checks → governance → lineage → metadata cataloguing**.

Built to demonstrate skills directly relevant to data management roles at financial institutions.

---

## 🗂️ Project Architecture

```
banking_data_platform/
├── etl/                        # Extraction, transformation, loading
│   ├── extractors/             # Upstream source connectors
│   ├── transformers/           # Business rule transformations
│   └── loaders/                # Target DB loaders
├── dbt_project/                # Dimensional modeling (star schema)
│   ├── models/staging/         # Raw-to-clean layer
│   ├── models/marts/           # Business-ready dimensional models
│   └── models/metrics/         # KPI aggregations
├── governance/                 # Data governance layer
│   ├── metadata/               # Dataset metadata catalog
│   ├── lineage/                # Column-level data lineage
│   └── data_dictionary/        # Business glossary
├── airflow/dags/               # Orchestration (pipeline scheduling)
├── sql/                        # DDL and analytical queries
├── data/                       # Simulated raw + processed datasets
├── notebooks/                  # EDA and data profiling
└── docs/                       # Architecture diagrams + ERDs
```

---

## 🎯 Skills Demonstrated

| Skill | Where |
|---|---|
| ETL pipeline development | `etl/`, `airflow/dags/` |
| Dimensional data modeling (ERD, star schema) | `dbt_project/models/`, `sql/ddl/` |
| Data profiling & quality reporting | `etl/transformers/data_quality.py` |
| Data governance & metadata management | `governance/` |
| Data lineage tracking | `governance/lineage/` |
| Upstream data acquisition & ingestion | `etl/extractors/` |
| Business rules & KPI tracking | `dbt_project/models/metrics/` |
| Pipeline orchestration | `airflow/dags/banking_pipeline_dag.py` |
| Data lifecycle management | End-to-end pipeline with archival logic |
| Stakeholder-facing documentation | `docs/`, `governance/data_dictionary/` |

---

## 🏗️ Data Architecture

### Source Systems (Upstream)
- **Core Banking System** — accounts, customers, balances
- **Transaction Processing System** — debit/credit transactions
- **Credit Risk System** — credit scores, loan data

### Target: Star Schema (Dimensional Model)
```
                    ┌─────────────────┐
                    │  FACT_TRANSACTIONS│
                    │  (grain: 1 txn)  │
                    └────────┬─────────┘
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
  │  DIM_CUSTOMER │  │  DIM_ACCOUNT │  │   DIM_DATE   │
  └──────────────┘  └──────────────┘  └──────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
```bash
python >= 3.10
PostgreSQL >= 14
Apache Airflow >= 2.7
dbt-postgres >= 1.6
```

### Installation
```bash
git clone https://github.com/your-username/banking-data-platform.git
cd banking-data-platform

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Database Setup
```bash
# Create DB and schema
psql -U postgres -f sql/ddl/01_create_schema.sql

# Generate synthetic banking data
python etl/extractors/generate_banking_data.py

# Run full ETL pipeline
python etl/run_pipeline.py

# Run dbt models
cd dbt_project && dbt run && dbt test
```

### Airflow Orchestration
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow dags trigger banking_retail_pipeline
```

---

## 📊 Data Quality Framework

Quality checks run at every pipeline stage:

| Check Type | Description | Action on Failure |
|---|---|---|
| Null check | Critical fields must not be null | Quarantine record |
| Referential integrity | FK relationships validated | Log + alert |
| Range validation | Amounts within expected bounds | Flag for review |
| Duplicate detection | Deduplicate on transaction_id | Drop duplicate |
| Schema drift | Column types match expectations | Halt pipeline |
| Freshness check | Data arrived within SLA window | Alert on-call |

Quality reports are written to `data/quality_reports/` after each run.

---

## 🗄️ Data Governance

### Metadata Catalog
Every dataset is registered in `governance/metadata/catalog.json` with:
- Owner, steward, classification (PII / Confidential / Internal)
- Retention policy
- Update frequency
- Upstream source system

### Data Lineage
Column-level lineage tracked in `governance/lineage/lineage_graph.py`:
```
core_banking.accounts.balance
    → staging.stg_accounts.current_balance
        → marts.dim_account.current_balance
            → metrics.kpi_daily_aum.total_aum
```

### Data Dictionary
Business glossary in `governance/data_dictionary/glossary.md` defines every business term used in the model (AUM, NII, delinquency rate, etc.)

---

## 📈 KPIs Tracked

- Daily transaction volume & value by account type
- Average balance per customer segment
- Transaction failure rate
- Customer lifetime value (CLV) proxy
- 30/60/90-day account dormancy flags

---

## 🔒 Compliance

- PII fields masked in non-production environments
- All data access logged for audit trail
- Retention policies enforced via lifecycle management scripts
- Aligns with PIPEDA (Canadian privacy legislation) data handling requirements

---

## 📄 License
MIT — free to use and adapt.
