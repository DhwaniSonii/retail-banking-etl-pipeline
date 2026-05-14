# Architecture & ERD Documentation

## System Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                         UPSTREAM SOURCE SYSTEMS                    │
│                                                                    │
│  ┌──────────────────┐  ┌─────────────────────┐  ┌───────────────┐  │
│  │  Core Banking    │  │ Transaction Process │  │  Credit Risk  │  │
│  │  System v4       │  │ System (TPS) v2     │  │  System v3    │  │
│  │                  │  │                     │  │               │  │
│  │  • Customers     │  │  • Debit/Credit     │  │  • Loans      │  │
│  │  • Accounts      │  │  • Transfers        │  │  • Scores     │  │
│  │  • Balances      │  │  • Bill Payments    │  │  • Risk Rtg   │  │
│  └────────┬─────────┘  └─────────────────────┘  └──────┬────────┘  │
└───────────┼──────────────────────┼─────────────────────┼───────────┘
            │                      │                     │
            ▼                      ▼                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER (Python ETL)                    │
│                  etl/extractors/generate_banking_data.py            │
│                                                                     │
│   • Simulates upstream API/file pulls                               │
│   • Registers each dataset to governance catalog                    │
│   • Writes to data/raw/ landing zone (CSV)                          │
│   • Intentionally injects DQ issues for validation testing          │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   DATA QUALITY GATE (Python)                        │
│                 etl/transformers/data_quality.py                    │
│                                                                     │
│   Dimensions checked:                                               │
│   ✓ Completeness  — null checks on critical fields                  │
│   ✓ Uniqueness    — duplicate detection on primary keys             │
│   ✓ Validity      — range checks, allowed values                    │
│   ✓ Referential   — FK integrity (account → customer)               │
│   ✓ Freshness     — SLA staleness detection                         │
│                                                                     │
│   Output: Quality score (0-100). Score < 70 → HALT pipeline         │
│   Quarantine: Bad records written to data/quarantine/               │
│   Reports:    JSON scorecards in data/quality_reports/              │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER (Python + dbt)                │
│                                                                     │
│   Python (etl/transformers/transform.py):                           │
│   • PII masking (SHA-256) in non-production environments            │
│   • Currency normalization (USD → CAD at spot rate)                 │
│   • Derived fields: age bands, dormancy flags, balance tiers        │
│   • Deduplication on primary keys                                   │
│   • Writes staged Parquet files to data/processed/                  │
│                                                                     │
│   dbt (dbt_project/models/):                                        │
│   • staging/ → clean, typed models (1:1 with source tables)         │
│   • marts/   → dimensional star schema (Kimball methodology)        │
│   • metrics/ → pre-aggregated KPI tables                            │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TARGET LAYER (PostgreSQL)                        │
│                                                                     │
│   Schema: staging.*  — Cleaned source-aligned tables                │
│   Schema: marts.*    — Star schema (fact + dims)                    │
│   Schema: metrics.*  — KPI aggregations                             │
│   Schema: governance.* — Catalog, lineage registry                  │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   GOVERNANCE LAYER (Python)                         │
│                                                                     │
│   • Metadata catalog (governance/metadata/catalog.json)             │
│   • Column-level lineage graph (governance/lineage/)                │
│   • Business data dictionary (governance/data_dictionary/)          │
│   • Lineage exported to JSON + Markdown on every run                │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│               ORCHESTRATION LAYER (Apache Airflow)                  │
│                airflow/dags/banking_pipeline_dag.py                 │
│                                                                     │
│   Schedule: Daily at 02:00 EST                                      │
│   Tasks:    extract → validate → [DQ gate] → transform → load       │
│             → dbt run → dbt test → compute KPIs → export lineage    │
│   Features: Retry logic, SLA alerts, branching on DQ failure        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Entity Relationship Diagram (ERD)

### Source Layer (Normalized / 3NF)

```
CUSTOMERS ──────────────────────────────────────────────────────────┐
│ customer_id     PK  VARCHAR(12)                                   │
│ first_name          VARCHAR(100)  [PII]                           │
│ last_name           VARCHAR(100)  [PII]                           │
│ email               VARCHAR(200)  [PII]                           │
│ date_of_birth       DATE          [PII]                           │
│ province            CHAR(2)                                       │
│ customer_segment    VARCHAR(50)                                   │
│ kyc_status          VARCHAR(20)                                   │
│ join_date           DATE                                          │
│ is_active           BOOLEAN                                       │
└───────────────────────────────────────────────────────────────────┘
         │ 1
         │
         │ M
ACCOUNTS ───────────────────────────────────────────────────────────┐
│ account_id      PK  VARCHAR(12)                                   │
│ customer_id     FK  VARCHAR(12) → CUSTOMERS.customer_id           │
│ account_type        VARCHAR(20)                                   │
│ account_status      VARCHAR(20)                                   │
│ current_balance     NUMERIC(18,2)                                 │
│ open_date           DATE                                          │
│ last_activity_date  DATE                                          │
│ branch_id           VARCHAR(10)                                   │
└───────────────────────────────────────────────────────────────────┘
         │ 1
         │
         │ M
TRANSACTIONS ───────────────────────────────────────────────────────┐
│ transaction_id  PK  VARCHAR(15)                                   │
│ account_id      FK  VARCHAR(12) → ACCOUNTS.account_id             │
│ transaction_type    VARCHAR(30)                                   │
│ amount              NUMERIC(18,2)                                 │
│ currency            CHAR(3)                                       │
│ transaction_date    DATE                                          │
│ status              VARCHAR(20)                                   │
│ merchant_category   VARCHAR(50)                                   │
└───────────────────────────────────────────────────────────────────┘
```

### Dimensional Layer (Star Schema — Kimball)

```
                         ┌───────────────────────────┐
                         │     FACT_TRANSACTIONS     │
                         │────────────────────────── │
                         │ transaction_sk   BIGSERIAL│ PK
                         │ transaction_id   VARCHAR  │ Natural Key
               ┌─────────┤ account_sk       INT      │ FK ──────────┐
               │         │ customer_sk      INT      │ FK ────────┐ │
               │    ┌────┤ date_sk          INT      │ FK ──────┐ │ │
               │    │    │ merchant_sk      INT      │ FK ──┐   │ │ │
               │    │    │ transaction_type VARCHAR  │      │   │ │ │
               │    │    │ channel          VARCHAR  │      │   │ │ │
               │    │    │ status           VARCHAR  │      │   │ │ │
               │    │    │ amount_cad       NUMERIC  │      │   │ │ │
               │    │    │ signed_amount_cad NUMERIC │      │   │ │ │
               │    │    │ is_large_txn     BOOLEAN  │      │   │ │ │
               │    │    └───────────────────────────┘      │   │ │ │
               │    │                                       │   │ │ │
               │    │    DIM_DATE ──────────────────────────┘   │ │ │
               │    └──► │ date_sk          INT (YYYYMMDD)  │ PK│ │ │
               │         │ full_date        DATE            │   │ │ │
               │         │ day_of_week      VARCHAR         │   │ │ │
               │         │ month_name       VARCHAR         │   │ │ │
               │         │ quarter          SMALLINT        │   │ │ │
               │         │ year             SMALLINT        │   │ │ │
               │         │ is_weekend       BOOLEAN         │   │ │ │
               │         │ is_holiday_ca    BOOLEAN         │   │ │ │
               │         └──────────────────────────────────┘   │ │ │
               │                                                │ │ │
               │         DIM_CUSTOMER ─────────────────────────-┘ │ │
               │    ┌───► │ customer_sk     SERIAL            │ PK  │ │
               │    │    │ customer_id     VARCHAR            │     │ │
               │    │    │ customer_segment VARCHAR           │     │ │
               │    │    │ kyc_status      VARCHAR            │     │ │
               │    │    │ province        CHAR(2)            │     │ │
               │    │    │ age_band        VARCHAR            │     │ │
               │    │    │ tenure_years    NUMERIC            │     │ │
               │    │    │ is_current      BOOLEAN  [SCD2]    │     │ │
               │    │    │ effective_from  DATE     [SCD2]    │     │ │
               │    │    │ effective_to    DATE     [SCD2]    │     │ │
               │    │    └────────────────────────────────────┘     │ │
               │    │                                               │ │
               │    │    DIM_ACCOUNT ───────────────────────────────┘ │
               └────┴──► │ account_sk      SERIAL             │ PK    │
                         │ account_id      VARCHAR            │       │
                         │ customer_sk     INT                │ FK    │
                         │ account_type    VARCHAR            │       │
                         │ balance_tier    VARCHAR            │       │
                         │ is_dormant      BOOLEAN            │       │
                         │ is_overdraft    BOOLEAN            │       │
                         │ is_current      BOOLEAN  [SCD2]    │       │
                         └────────────────────────────────────┘       │
                                                                      │
                         DIM_MERCHANT ────────────────────────────────┘
                    ┌──► │ merchant_sk     SERIAL             │ PK
                    │    │ merchant_category VARCHAR          │
                    │    │ category_group  VARCHAR            │
                    │    │ is_essential    BOOLEAN            │
                    │    └────────────────────────────────────┘
```

---

## Data Flow & Lineage Summary

```
CORE_BANKING.customers → stg_customers → dim_customer → fact_transactions → kpi_daily_summary
CORE_BANKING.accounts  → stg_accounts  → dim_account  → fact_transactions → kpi_customer_segment
TPS.transactions       → stg_transactions              → fact_transactions → kpi_daily_summary
CREDIT_RISK.loans      → stg_credit    (standalone — feeds risk reporting)
```

