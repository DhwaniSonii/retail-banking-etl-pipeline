-- ============================================================
-- Banking Data Platform — Star Schema DDL
-- Target: PostgreSQL 14+
--
-- Schema design: Kimball dimensional modeling methodology
--
-- ERD Overview:
--
--                    ┌─────────────────────┐
--                    │   FACT_TRANSACTIONS  │  ← grain: one row per transaction
--                    │─────────────────────│
--                    │ transaction_sk (PK) │
--                    │ account_sk (FK) ────┼──→ DIM_ACCOUNT
--                    │ customer_sk (FK) ───┼──→ DIM_CUSTOMER
--                    │ date_sk (FK) ───────┼──→ DIM_DATE
--                    │ merchant_sk (FK) ───┼──→ DIM_MERCHANT
--                    │ amount_cad          │
--                    │ signed_amount_cad   │
--                    │ is_large_transaction│
--                    └─────────────────────┘
--
-- ============================================================

-- ── Schemas ────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS staging;   -- Raw-to-clean layer
CREATE SCHEMA IF NOT EXISTS marts;     -- Business-ready dimensional model
CREATE SCHEMA IF NOT EXISTS metrics;   -- KPI aggregations
CREATE SCHEMA IF NOT EXISTS governance;-- Metadata, lineage, data dictionary

-- ── Staging Layer ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id         VARCHAR(12) NOT NULL,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(200),
    phone               VARCHAR(50),
    date_of_birth       DATE,
    province            CHAR(2),
    postal_code         VARCHAR(10),
    customer_segment    VARCHAR(50),
    kyc_status          VARCHAR(20),
    join_date           DATE,
    is_active           BOOLEAN,
    age_years           SMALLINT,
    tenure_years        NUMERIC(5,1),
    age_band            VARCHAR(20),
    _transformed_at     TIMESTAMP,
    _pipeline_version   VARCHAR(10),
    CONSTRAINT stg_customers_pk PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_accounts (
    account_id          VARCHAR(12) NOT NULL,
    customer_id         VARCHAR(12) NOT NULL,
    account_type        VARCHAR(20),
    account_status      VARCHAR(20),
    currency            CHAR(3),
    current_balance     NUMERIC(18,2),
    available_balance   NUMERIC(18,2),
    interest_rate       NUMERIC(6,4),
    open_date           DATE,
    last_activity_date  DATE,
    branch_id           VARCHAR(10),
    account_age_days    INTEGER,
    days_since_activity INTEGER,
    is_dormant          BOOLEAN,
    balance_tier        VARCHAR(30),
    is_overdraft        BOOLEAN,
    _transformed_at     TIMESTAMP,
    _pipeline_version   VARCHAR(10),
    CONSTRAINT stg_accounts_pk PRIMARY KEY (account_id),
    CONSTRAINT stg_accounts_customer_fk FOREIGN KEY (customer_id)
        REFERENCES staging.stg_customers(customer_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_transactions (
    transaction_id        VARCHAR(15) NOT NULL,
    account_id            VARCHAR(12) NOT NULL,
    transaction_type      VARCHAR(30),
    channel               VARCHAR(20),
    amount                NUMERIC(18,2),
    currency              CHAR(3),
    amount_cad            NUMERIC(18,2),
    signed_amount_cad     NUMERIC(18,2),
    merchant_category     VARCHAR(50),
    transaction_date      DATE,
    transaction_time      TIME,
    status                VARCHAR(20),
    description           TEXT,
    txn_year              SMALLINT,
    txn_month             SMALLINT,
    txn_quarter           SMALLINT,
    txn_day_of_week       VARCHAR(12),
    txn_week              SMALLINT,
    is_weekend            BOOLEAN,
    is_large_transaction  BOOLEAN,
    _transformed_at       TIMESTAMP,
    _pipeline_version     VARCHAR(10),
    CONSTRAINT stg_transactions_pk PRIMARY KEY (transaction_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_credit (
    loan_id              VARCHAR(12) NOT NULL,
    customer_id          VARCHAR(12) NOT NULL,
    loan_type            VARCHAR(30),
    original_amount      NUMERIC(18,2),
    outstanding_balance  NUMERIC(18,2),
    interest_rate        NUMERIC(6,4),
    term_months          SMALLINT,
    origination_date     DATE,
    maturity_date        DATE,
    credit_score         SMALLINT,
    risk_rating          VARCHAR(5),
    days_past_due        SMALLINT,
    loan_status          VARCHAR(20),
    collateral_type      VARCHAR(20),
    loan_age_months      SMALLINT,
    months_to_maturity   SMALLINT,
    ltv_ratio            NUMERIC(6,4),
    credit_score_band    VARCHAR(30),
    is_delinquent        BOOLEAN,
    delinquency_bucket   VARCHAR(20),
    _transformed_at      TIMESTAMP,
    _pipeline_version    VARCHAR(10),
    CONSTRAINT stg_credit_pk PRIMARY KEY (loan_id)
);

-- ── Dimensional Layer (marts) ────────────────────────────────

CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_sk         INTEGER NOT NULL,   -- YYYYMMDD surrogate key
    full_date       DATE NOT NULL,
    day_of_week     VARCHAR(12),
    day_of_month    SMALLINT,
    week_of_year    SMALLINT,
    month_number    SMALLINT,
    month_name      VARCHAR(12),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday_ca   BOOLEAN,            -- Canadian statutory holiday flag
    fiscal_period   VARCHAR(10),        -- TD fiscal year aligns with calendar year
    CONSTRAINT dim_date_pk PRIMARY KEY (date_sk)
);

CREATE TABLE IF NOT EXISTS marts.dim_customer (
    customer_sk       SERIAL NOT NULL,
    customer_id       VARCHAR(12) NOT NULL,
    customer_segment  VARCHAR(50),
    kyc_status        VARCHAR(20),
    province          CHAR(2),
    age_band          VARCHAR(20),
    tenure_years      NUMERIC(5,1),
    is_active         BOOLEAN,
    -- SCD Type 2 columns
    effective_from    DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to      DATE,
    is_current        BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at        TIMESTAMP DEFAULT NOW(),
    CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk)
);

CREATE INDEX IF NOT EXISTS dim_customer_id_idx ON marts.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS dim_customer_current_idx ON marts.dim_customer(customer_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS marts.dim_account (
    account_sk          SERIAL NOT NULL,
    account_id          VARCHAR(12) NOT NULL,
    customer_sk         INTEGER NOT NULL,
    account_type        VARCHAR(20),
    account_status      VARCHAR(20),
    currency            CHAR(3),
    balance_tier        VARCHAR(30),
    branch_id           VARCHAR(10),
    is_dormant          BOOLEAN,
    is_overdraft        BOOLEAN,
    open_date           DATE,
    -- SCD Type 2 columns
    effective_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to        DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at          TIMESTAMP DEFAULT NOW(),
    CONSTRAINT dim_account_pk PRIMARY KEY (account_sk),
    CONSTRAINT dim_account_customer_fk FOREIGN KEY (customer_sk)
        REFERENCES marts.dim_customer(customer_sk)
);

CREATE INDEX IF NOT EXISTS dim_account_id_idx ON marts.dim_account(account_id);

CREATE TABLE IF NOT EXISTS marts.dim_merchant (
    merchant_sk       SERIAL NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    category_group    VARCHAR(30),   -- e.g. "Food & Beverage", "Transportation"
    is_essential      BOOLEAN,
    _loaded_at        TIMESTAMP DEFAULT NOW(),
    CONSTRAINT dim_merchant_pk PRIMARY KEY (merchant_sk),
    CONSTRAINT dim_merchant_category_uq UNIQUE (merchant_category)
);

CREATE TABLE IF NOT EXISTS marts.fact_transactions (
    transaction_sk        BIGSERIAL NOT NULL,
    transaction_id        VARCHAR(15) NOT NULL,
    account_sk            INTEGER NOT NULL,
    customer_sk           INTEGER NOT NULL,
    date_sk               INTEGER NOT NULL,
    merchant_sk           INTEGER,
    transaction_type      VARCHAR(30),
    channel               VARCHAR(20),
    status                VARCHAR(20),
    amount_cad            NUMERIC(18,2),
    signed_amount_cad     NUMERIC(18,2),
    is_large_transaction  BOOLEAN,
    is_weekend            BOOLEAN,
    _loaded_at            TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fact_transactions_pk PRIMARY KEY (transaction_sk),
    CONSTRAINT fact_txn_account_fk   FOREIGN KEY (account_sk)   REFERENCES marts.dim_account(account_sk),
    CONSTRAINT fact_txn_customer_fk  FOREIGN KEY (customer_sk)  REFERENCES marts.dim_customer(customer_sk),
    CONSTRAINT fact_txn_date_fk      FOREIGN KEY (date_sk)      REFERENCES marts.dim_date(date_sk),
    CONSTRAINT fact_txn_merchant_fk  FOREIGN KEY (merchant_sk)  REFERENCES marts.dim_merchant(merchant_sk)
);

CREATE INDEX IF NOT EXISTS fact_txn_date_idx       ON marts.fact_transactions(date_sk);
CREATE INDEX IF NOT EXISTS fact_txn_account_idx    ON marts.fact_transactions(account_sk);
CREATE INDEX IF NOT EXISTS fact_txn_customer_idx   ON marts.fact_transactions(customer_sk);
CREATE INDEX IF NOT EXISTS fact_txn_large_idx      ON marts.fact_transactions(is_large_transaction) WHERE is_large_transaction;

-- ── Metrics Layer ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS metrics.kpi_daily_summary (
    summary_date          DATE NOT NULL,
    transaction_count     INTEGER,
    total_volume_cad      NUMERIC(22,2),
    avg_transaction_cad   NUMERIC(18,2),
    unique_accounts       INTEGER,
    unique_customers      INTEGER,
    failure_rate_pct      NUMERIC(6,2),
    large_txn_count       INTEGER,
    _computed_at          TIMESTAMP DEFAULT NOW(),
    CONSTRAINT kpi_daily_pk PRIMARY KEY (summary_date)
);

CREATE TABLE IF NOT EXISTS metrics.kpi_customer_segment (
    summary_date          DATE NOT NULL,
    customer_segment      VARCHAR(50) NOT NULL,
    active_accounts       INTEGER,
    avg_balance_cad       NUMERIC(18,2),
    total_transactions    INTEGER,
    dormant_accounts      INTEGER,
    _computed_at          TIMESTAMP DEFAULT NOW(),
    CONSTRAINT kpi_segment_pk PRIMARY KEY (summary_date, customer_segment)
);

-- ── Governance Schema ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS governance.data_catalog (
    catalog_id          SERIAL NOT NULL,
    dataset_name        VARCHAR(100) NOT NULL UNIQUE,
    source_system       VARCHAR(100),
    owner               VARCHAR(100),
    steward             VARCHAR(100),
    classification      VARCHAR(50),
    update_frequency    VARCHAR(50),
    retention_policy    VARCHAR(100),
    row_count           BIGINT,
    last_updated        TIMESTAMP DEFAULT NOW(),
    CONSTRAINT data_catalog_pk PRIMARY KEY (catalog_id)
);

CREATE TABLE IF NOT EXISTS governance.data_lineage (
    lineage_id          SERIAL NOT NULL,
    source_dataset      VARCHAR(100) NOT NULL,
    source_column       VARCHAR(100) NOT NULL,
    target_dataset      VARCHAR(100) NOT NULL,
    target_column       VARCHAR(100) NOT NULL,
    transformation_rule TEXT,
    pipeline_step       VARCHAR(100),
    recorded_at         TIMESTAMP DEFAULT NOW(),
    CONSTRAINT data_lineage_pk PRIMARY KEY (lineage_id)
);

COMMENT ON TABLE marts.fact_transactions IS 'Central fact table. Grain: one row per transaction. All monetary amounts in CAD.';
COMMENT ON TABLE marts.dim_customer IS 'Customer dimension with SCD Type 2 history tracking.';
COMMENT ON TABLE marts.dim_account IS 'Account dimension with SCD Type 2 history tracking.';
COMMENT ON TABLE marts.dim_date IS 'Conformed date dimension. Populate 10 years forward.';
COMMENT ON TABLE governance.data_lineage IS 'Column-level data lineage registry.';
