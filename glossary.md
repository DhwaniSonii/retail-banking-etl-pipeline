# Data Dictionary — Banking Data Platform

Business glossary for all terms, metrics, and fields used in the platform.
Maintained by the Data Management Office. Every term maps to at least one dataset column.

---

## Business Terms

| Term | Definition | Source System | Dataset.Column |
|---|---|---|---|
| **AUM** | Assets Under Management — total client balances across all deposit accounts | Core Banking | stg_accounts.current_balance (SUM) |
| **NII** | Net Interest Income — difference between interest earned on loans and paid on deposits | Credit Risk + Core Banking | stg_credit.interest_rate, stg_accounts.interest_rate |
| **DPD** | Days Past Due — number of calendar days a loan payment is overdue | Credit Risk System | stg_credit.days_past_due |
| **LTV** | Loan-to-Value ratio — outstanding loan balance divided by original loan amount | Derived | stg_credit.ltv_ratio |
| **FINTRAC** | Financial Transactions and Reports Analysis Centre of Canada — federal regulator requiring reporting of cash transactions ≥ CAD $10,000 | Compliance | stg_transactions.is_large_transaction |
| **KYC** | Know Your Customer — regulatory requirement to verify client identity | Core Banking | stg_customers.kyc_status |
| **SCD Type 2** | Slowly Changing Dimension Type 2 — history-preserving pattern that adds a new row when a dimension attribute changes | Data Architecture | dim_customer, dim_account |
| **Dormant Account** | Account with no customer-initiated activity for 730+ consecutive days (TD policy threshold) | Business Rule | stg_accounts.is_dormant |
| **Surrogate Key** | System-generated integer key (_sk suffix) used as dimension primary key; independent of natural business key | Data Architecture | dim_customer.customer_sk, dim_account.account_sk |
| **Grain** | The level of detail represented by a single row in a fact table. fact_transactions grain = one financial transaction | Data Architecture | marts.fact_transactions |
| **CLV** | Customer Lifetime Value — estimated total revenue a customer generates over their relationship with the bank | Derived / Analytics | Computed in metrics layer |
| **Delinquency Bucket** | Grouping of loans by days past due: Current / 1-30 / 31-60 / 61-90 / 91-180 / 180+ DPD | Business Rule | stg_credit.delinquency_bucket |
| **PII** | Personally Identifiable Information — data that can identify a specific individual; masked in non-production environments | Governance | stg_customers: first_name, last_name, email, phone, postal_code, date_of_birth |
| **Staging Layer** | Intermediate schema holding cleaned, type-cast, and deduplicated data before dimensional modeling | Architecture | staging.* |
| **Mart** | Business-facing dimensional schema designed for analytical queries | Architecture | marts.* |
| **ETL** | Extract, Transform, Load — the process of moving data from source systems through transformation into the target warehouse | Architecture | etl/ pipeline |
| **Data Lineage** | Documented chain of custody showing how a data field flows from its origin through every transformation to its final form | Governance | governance/lineage/ |
| **PIPEDA** | Personal Information Protection and Electronic Documents Act — Canadian federal privacy legislation governing how organizations handle personal data | Compliance | All PII handling |
| **OSFI B-20** | Office of the Superintendent of Financial Institutions Guideline B-20 — residential mortgage underwriting standards requiring 10-year data retention | Compliance | stg_credit.* (10-year retention) |

---

## Column Glossary

### stg_customers

| Column | Business Definition | Type | Nullable | Example |
|---|---|---|---|---|
| customer_id | Unique identifier assigned by Core Banking at account opening. Format: CUST + 7 digits | VARCHAR(12) | No | CUST0001234 |
| customer_segment | TD internal segmentation: Mass Market, Affluent, Private Banking, Small Business | VARCHAR(50) | Yes | Affluent |
| kyc_status | Regulatory verification status. Verified = passed identity checks. Expired = re-verification required | VARCHAR(20) | No | Verified |
| age_band | Derived age grouping for segmentation analysis: 18-25, 26-35, 36-50, 51-65, 65+ | VARCHAR(20) | Yes | 36-50 |
| tenure_years | Number of years the customer has held an account, rounded to 1 decimal | NUMERIC(5,1) | Yes | 7.3 |

### stg_accounts

| Column | Business Definition | Type | Nullable | Example |
|---|---|---|---|---|
| account_id | Unique account identifier. Format: ACC + 8 digits | VARCHAR(12) | No | ACC00012345 |
| balance_tier | Derived balance grouping: Negative / Under $1K / $1K–$10K / $10K–$50K / $50K–$100K / Over $100K | VARCHAR(30) | Yes | $10K–$50K |
| is_dormant | TRUE if account has had no customer-initiated activity for >730 days AND status is Active | BOOLEAN | No | FALSE |
| is_overdraft | TRUE if current_balance < 0 | BOOLEAN | No | FALSE |
| days_since_activity | Calendar days elapsed since last_activity_date as of pipeline run date | INTEGER | Yes | 45 |

### stg_transactions

| Column | Business Definition | Type | Nullable | Example |
|---|---|---|---|---|
| transaction_id | Unique transaction identifier from TPS. Format: TXN + 10 digits | VARCHAR(15) | No | TXN0000000123 |
| amount_cad | Transaction amount converted to Canadian dollars. USD amounts multiplied by spot rate | NUMERIC(18,2) | No | 142.50 |
| signed_amount_cad | Signed amount: negative for Debit/ATM/Bill Payment, positive for Credit/Transfer | NUMERIC(18,2) | No | -142.50 |
| is_large_transaction | TRUE if amount_cad ≥ $10,000 (FINTRAC reporting threshold) | BOOLEAN | No | FALSE |
| merchant_category | MCC-derived spending category. NULL replaced with 'Unclassified' | VARCHAR(50) | No | Grocery |

### stg_credit

| Column | Business Definition | Type | Nullable | Example |
|---|---|---|---|---|
| loan_id | Unique loan identifier from Credit Risk System. Format: LN + 8 digits | VARCHAR(12) | No | LN00001234 |
| ltv_ratio | Loan-to-value: outstanding_balance / original_amount. Ranges 0.00–1.00 | NUMERIC(6,4) | Yes | 0.6230 |
| credit_score_band | Derived Equifax-aligned band: Poor (<580) / Fair / Good / Very Good / Excellent (800+) | VARCHAR(30) | Yes | Good (670-739) |
| delinquency_bucket | DPD grouping per OSFI classification: Current / 1-30 DPD / 31-60 / 61-90 / 91-180 / 180+ DPD | VARCHAR(20) | No | Current |
| risk_rating | Internal credit risk rating aligned to S&P scale: AAA through CCC | VARCHAR(5) | Yes | BBB |

---

## Retention Schedule

| Dataset | Retention Period | Authority |
|---|---|---|
| Customer PII | 7 years after account closure | FINTRAC |
| Transaction records | 7 years | FINTRAC |
| Credit / Loan data | 10 years | OSFI B-20 |
| Data quality reports | 3 years | Internal policy |
| Pipeline audit logs | 5 years | Internal policy |

---

*Last updated: 2024-12-31 | Owner: Data Management Office | Steward: Platform Data Team*
