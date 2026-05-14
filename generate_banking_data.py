# generates synthetic banking data for pipeline testing
# pulls from 3 source systems - core banking, payments, credit risk
# saves to data/raw/ and logs to governance catalog

import os
import json
import random
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("extractor.banking")

fake = Faker("en_CA")
rng = np.random.default_rng(seed=42)

RAW_DIR = Path(__file__).parents[2] / "data" / "raw"
CATALOG_PATH = Path(__file__).parents[2] / "governance" / "metadata" / "catalog.json"
RAW_DIR.mkdir(parents=True, exist_ok=True)
CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# Constants
ACCOUNT_TYPES = ["Chequing", "Savings", "TFSA", "RRSP", "GIC"]
TRANSACTION_TYPES = ["Debit", "Credit", "Transfer", "Bill Payment", "ATM Withdrawal"]
TRANSACTION_CHANNELS = ["Online", "Branch", "ATM", "Mobile", "POS"]
PROVINCES = ["ON", "BC", "AB", "QC", "MB", "SK", "NS"]
LOAN_TYPES = ["Personal", "Mortgage", "Auto", "Line of Credit"]
CREDIT_RISK_RATINGS = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]

N_CUSTOMERS = 5_000
N_ACCOUNTS = 8_000
N_TRANSACTIONS = 200_000
N_LOANS = 1_500
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)


# Helpers

def _random_dates(start: datetime, end: datetime, n: int) -> list[datetime]:
    delta = (end - start).days
    return [start + timedelta(days=int(rng.integers(0, delta))) for _ in range(n)]


def _mask_pii(value: str) -> str:
    """SHA-256 hash for PII masking in non-production environments."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _register_to_catalog(dataset_name: str, meta: dict) -> None:
    """Append dataset metadata to the governance catalog."""
    try:
        with open(CATALOG_PATH) as f:
            catalog = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        catalog = {"datasets": []}

    # Upsert: replace existing entry if dataset_name already registered
    catalog["datasets"] = [d for d in catalog["datasets"] if d["name"] != dataset_name]
    catalog["datasets"].append({"name": dataset_name, **meta})

    with open(CATALOG_PATH, "w") as f:
        json.dump(catalog, f, indent=2, default=str)

    log.info("Catalog updated → %s", dataset_name)



#Source System 1: Core Banking — Customers

def extract_customers() -> pd.DataFrame:
    log.info("Extracting customers from Core Banking System (%d records)…", N_CUSTOMERS)

    customer_ids = [f"CUST{str(i).zfill(7)}" for i in range(1, N_CUSTOMERS + 1)]
    join_dates = _random_dates(datetime(2000, 1, 1), END_DATE, N_CUSTOMERS)

    df = pd.DataFrame({
        "customer_id":       customer_ids,
        "first_name":        [fake.first_name() for _ in range(N_CUSTOMERS)],
        "last_name":         [fake.last_name() for _ in range(N_CUSTOMERS)],
        "email":             [fake.email() for _ in range(N_CUSTOMERS)],
        "phone":             [fake.phone_number() for _ in range(N_CUSTOMERS)],
        "date_of_birth":     [fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat()
                               for _ in range(N_CUSTOMERS)],
        "province":          rng.choice(PROVINCES, N_CUSTOMERS),
        "postal_code":       [fake.postcode() for _ in range(N_CUSTOMERS)],
        "customer_segment":  rng.choice(["Mass Market", "Affluent", "Private Banking", "Small Business"],
                                         N_CUSTOMERS, p=[0.6, 0.25, 0.05, 0.10]),
        "kyc_status":        rng.choice(["Verified", "Pending", "Expired"], N_CUSTOMERS, p=[0.90, 0.07, 0.03]),
        "join_date":         [d.date().isoformat() for d in join_dates],
        "is_active":         rng.choice([True, False], N_CUSTOMERS, p=[0.92, 0.08]),
        "_source_system":    "CORE_BANKING_v4",
        "_extract_ts":       datetime.utcnow().isoformat(),
    })

    out = RAW_DIR / "customers_raw.csv"
    df.to_csv(out, index=False)
    log.info("Customers written → %s (%d rows)", out, len(df))

    _register_to_catalog("customers_raw", {
        "source_system": "Core Banking System",
        "owner": "Retail Banking Platform",
        "steward": "Data Management Office",
        "classification": "PII — Confidential",
        "pii_fields": ["first_name", "last_name", "email", "phone", "date_of_birth", "postal_code"],
        "row_count": len(df),
        "columns": list(df.columns),
        "retention_policy": "7 years (FINTRAC)",
        "update_frequency": "Daily",
        "landing_path": str(out),
        "extracted_at": datetime.utcnow().isoformat(),
    })

    return df


# Source System 1: Core Banking — Accounts

def extract_accounts(customer_ids: list[str]) -> pd.DataFrame:
    log.info("Extracting accounts from Core Banking System (%d records)…", N_ACCOUNTS)

    # Each customer can have 1–3 accounts
    account_customer_ids = rng.choice(customer_ids, N_ACCOUNTS)
    open_dates = _random_dates(datetime(2005, 1, 1), END_DATE, N_ACCOUNTS)

    balances = np.where(
        rng.random(N_ACCOUNTS) < 0.03,
        rng.uniform(-500, 0, N_ACCOUNTS),           # 3% overdraft
        rng.exponential(scale=15_000, size=N_ACCOUNTS),  # typical balances
    ).round(2)

    df = pd.DataFrame({
        "account_id":         [f"ACC{str(i).zfill(8)}" for i in range(1, N_ACCOUNTS + 1)],
        "customer_id":        account_customer_ids,
        "account_type":       rng.choice(ACCOUNT_TYPES, N_ACCOUNTS, p=[0.35, 0.30, 0.15, 0.15, 0.05]),
        "account_status":     rng.choice(["Active", "Dormant", "Closed", "Frozen"],
                                          N_ACCOUNTS, p=[0.82, 0.08, 0.07, 0.03]),
        "currency":           rng.choice(["CAD", "USD"], N_ACCOUNTS, p=[0.88, 0.12]),
        "current_balance":    balances,
        "available_balance":  (balances * rng.uniform(0.95, 1.0, N_ACCOUNTS)).round(2),
        "interest_rate":      rng.uniform(0.01, 5.50, N_ACCOUNTS).round(4),
        "open_date":          [d.date().isoformat() for d in open_dates],
        "last_activity_date": [
            (d + timedelta(days=int(rng.integers(0, (END_DATE - d).days + 1)))).date().isoformat()
            if (END_DATE - d).days > 0 else d.date().isoformat()
            for d in open_dates
        ],
        "branch_id":          [f"BR{str(rng.integers(1, 500)):>04}" for _ in range(N_ACCOUNTS)],
        "_source_system":     "CORE_BANKING_v4",
        "_extract_ts":        datetime.utcnow().isoformat(),
    })

    out = RAW_DIR / "accounts_raw.csv"
    df.to_csv(out, index=False)
    log.info("Accounts written → %s (%d rows)", out, len(df))

    _register_to_catalog("accounts_raw", {
        "source_system": "Core Banking System",
        "owner": "Retail Banking Platform",
        "steward": "Data Management Office",
        "classification": "Confidential",
        "pii_fields": [],
        "row_count": len(df),
        "columns": list(df.columns),
        "retention_policy": "7 years (FINTRAC)",
        "update_frequency": "Daily",
        "landing_path": str(out),
        "extracted_at": datetime.utcnow().isoformat(),
    })

    return df


# Source System 2: Transaction Processing System 

def extract_transactions(account_ids: list[str]) -> pd.DataFrame:
    log.info("Extracting transactions from TPS (%d records)…", N_TRANSACTIONS)

    txn_dates = _random_dates(START_DATE, END_DATE, N_TRANSACTIONS)

    # Introduce intentional data quality issues for the quality checker to catch
    amounts = rng.exponential(scale=250, size=N_TRANSACTIONS).round(2)
    amounts[rng.integers(0, N_TRANSACTIONS, size=50)] = np.nan       # 50 nulls
    amounts[rng.integers(0, N_TRANSACTIONS, size=20)] = -99_999_999  # 20 outliers

    account_sample = rng.choice(account_ids, N_TRANSACTIONS)
    # Inject 30 orphan account IDs (referential integrity violation)
    orphan_idx = rng.integers(0, N_TRANSACTIONS, size=30)
    for idx in orphan_idx:
        account_sample[idx] = "ACC_INVALID_9999"

    txn_ids = [f"TXN{str(i).zfill(10)}" for i in range(1, N_TRANSACTIONS + 1)]
    # Inject 15 duplicate transaction IDs
    dup_idx = rng.integers(0, N_TRANSACTIONS, size=15)
    for idx in dup_idx:
        txn_ids[idx] = txn_ids[rng.integers(0, idx + 1)]

    df = pd.DataFrame({
        "transaction_id":    txn_ids,
        "account_id":        account_sample,
        "transaction_type":  rng.choice(TRANSACTION_TYPES, N_TRANSACTIONS, p=[0.35, 0.30, 0.15, 0.12, 0.08]),
        "channel":           rng.choice(TRANSACTION_CHANNELS, N_TRANSACTIONS),
        "amount":            amounts,
        "currency":          rng.choice(["CAD", "USD"], N_TRANSACTIONS, p=[0.88, 0.12]),
        "merchant_category": rng.choice(
            ["Grocery", "Restaurant", "Gas", "Utilities", "Entertainment", "Travel", "Healthcare", "Retail", None],
            N_TRANSACTIONS, p=[0.15, 0.12, 0.10, 0.08, 0.08, 0.07, 0.05, 0.15, 0.20]
        ),
        "transaction_date":  [d.date().isoformat() for d in txn_dates],
        "transaction_time":  [f"{rng.integers(0,24):02}:{rng.integers(0,60):02}:{rng.integers(0,60):02}"
                               for _ in range(N_TRANSACTIONS)],
        "status":            rng.choice(["Completed", "Pending", "Failed", "Reversed"],
                                         N_TRANSACTIONS, p=[0.91, 0.04, 0.03, 0.02]),
        "description":       [fake.sentence(nb_words=4) for _ in range(N_TRANSACTIONS)],
        "_source_system":    "TPS_PAYMENTS_v2",
        "_extract_ts":       datetime.utcnow().isoformat(),
    })

    out = RAW_DIR / "transactions_raw.csv"
    df.to_csv(out, index=False)
    log.info("Transactions written → %s (%d rows)", out, len(df))

    _register_to_catalog("transactions_raw", {
        "source_system": "Transaction Processing System",
        "owner": "Payments Platform",
        "steward": "Data Management Office",
        "classification": "Confidential",
        "pii_fields": [],
        "row_count": len(df),
        "columns": list(df.columns),
        "retention_policy": "7 years (FINTRAC)",
        "update_frequency": "Real-time (batch micro-batch every 15 min)",
        "landing_path": str(out),
        "extracted_at": datetime.utcnow().isoformat(),
        "intentional_dq_issues": {
            "null_amounts": 50,
            "amount_outliers": 20,
            "orphan_account_ids": 30,
            "duplicate_transaction_ids": 15,
        }
    })

    return df


# ── Source System 3: Credit Risk System ───────────────────────────────────────

def extract_credit_data(customer_ids: list[str]) -> pd.DataFrame:
    n = N_LOANS
    log.info("Extracting credit data from Risk System (%d records)…", n)

    sample_customers = rng.choice(customer_ids, n, replace=False)
    origination_dates = _random_dates(datetime(2015, 1, 1), END_DATE, n)

    loan_amounts = rng.choice(
        [rng.uniform(1_000, 50_000, n),
         rng.uniform(50_000, 1_500_000, n),
         rng.uniform(5_000, 80_000, n)],
    ).diagonal().round(2)

    df = pd.DataFrame({
        "loan_id":              [f"LN{str(i).zfill(8)}" for i in range(1, n + 1)],
        "customer_id":          sample_customers,
        "loan_type":            rng.choice(LOAN_TYPES, n, p=[0.25, 0.45, 0.20, 0.10]),
        "original_amount":      loan_amounts,
        "outstanding_balance":  (loan_amounts * rng.uniform(0.0, 1.0, n)).round(2),
        "interest_rate":        rng.uniform(2.50, 19.99, n).round(4),
        "term_months":          rng.choice([12, 24, 36, 48, 60, 120, 240, 300], n),
        "origination_date":     [d.date().isoformat() for d in origination_dates],
        "maturity_date":        [
            (d + timedelta(days=int(rng.integers(365, 365 * 26)))).date().isoformat()
            for d in origination_dates
        ],
        "credit_score":         rng.integers(300, 900, n),
        "risk_rating":          rng.choice(CREDIT_RISK_RATINGS, n, p=[0.05, 0.10, 0.25, 0.30, 0.15, 0.10, 0.05]),
        "days_past_due":        np.where(rng.random(n) < 0.08, rng.integers(1, 180, n), 0),
        "loan_status":          rng.choice(["Current", "Delinquent", "Default", "Paid Off"],
                                            n, p=[0.80, 0.08, 0.03, 0.09]),
        "collateral_type":      rng.choice(["Property", "Vehicle", "None", "Other"], n, p=[0.45, 0.20, 0.30, 0.05]),
        "_source_system":       "CREDIT_RISK_SYSTEM_v3",
        "_extract_ts":          datetime.utcnow().isoformat(),
    })

    out = RAW_DIR / "credit_data_raw.csv"
    df.to_csv(out, index=False)
    log.info("Credit data written → %s (%d rows)", out, len(df))

    _register_to_catalog("credit_data_raw", {
        "source_system": "Credit Risk System",
        "owner": "Risk & Analytics",
        "steward": "Credit Risk Data Office",
        "classification": "Confidential — Regulatory",
        "pii_fields": [],
        "row_count": len(df),
        "columns": list(df.columns),
        "retention_policy": "10 years (OSFI B-20 Guideline)",
        "update_frequency": "Daily EOD",
        "landing_path": str(out),
        "extracted_at": datetime.utcnow().isoformat(),
    })

    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def run_extraction() -> dict[str, pd.DataFrame]:
    log.info("═" * 60)
    log.info("BANKING DATA PLATFORM — EXTRACTION PHASE")
    log.info("═" * 60)

    customers_df = extract_customers()
    accounts_df = extract_accounts(customers_df["customer_id"].tolist())
    transactions_df = extract_transactions(accounts_df["account_id"].tolist())
    credit_df = extract_credit_data(customers_df["customer_id"].tolist())

    log.info("═" * 60)
    log.info("Extraction complete. Summary:")
    log.info("  Customers    : %d", len(customers_df))
    log.info("  Accounts     : %d", len(accounts_df))
    log.info("  Transactions : %d", len(transactions_df))
    log.info("  Loans        : %d", len(credit_df))
    log.info("  Catalog      : %s", CATALOG_PATH)
    log.info("═" * 60)

    return {
        "customers": customers_df,
        "accounts": accounts_df,
        "transactions": transactions_df,
        "credit": credit_df,
    }


if __name__ == "__main__":
    run_extraction()
