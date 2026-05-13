"""
ETL Transformer — Business Rules & Staging Layer

Applies business rules and transformations to raw banking data.
Produces clean, conformed datasets ready for dimensional modeling.

Business rules applied:
  - PII masking for non-production environments
  - Currency normalization (USD → CAD)
  - Derived fields (account age, dormancy flags, customer segments)
  - Deduplication
  - Null imputation strategies per field
  - Data type enforcement
"""

from __future__ import annotations

import logging
import hashlib
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import numpy as np

log = logging.getLogger("etl.transformer")

PROCESSED_DIR = Path(__file__).parents[2] / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# CAD/USD spot rate — in production this would call a rates API or lookup table
USD_TO_CAD_RATE = 1.36

# Dormancy threshold in days (TD policy: 2 years)
DORMANCY_THRESHOLD_DAYS = 730


# ── PII Handling ───────────────────────────────────────────────────────────────

def mask_pii(value: str) -> str:
    """Deterministic SHA-256 mask — same input always produces same output (for joins)."""
    if pd.isna(value):
        return None
    return "MASKED_" + hashlib.sha256(str(value).encode()).hexdigest()[:12].upper()


def apply_pii_masking(df: pd.DataFrame, pii_columns: list[str], environment: str = "dev") -> pd.DataFrame:
    """Mask PII fields in non-production environments."""
    if environment == "prod":
        log.info("Production environment — PII columns left unmasked for authorized access only.")
        return df

    df = df.copy()
    for col in pii_columns:
        if col in df.columns:
            df[col] = df[col].apply(mask_pii)
            log.info("PII masked → column: %s", col)
    return df


# ── Currency Normalization ─────────────────────────────────────────────────────

def normalize_currency(df: pd.DataFrame, amount_col: str, currency_col: str) -> pd.DataFrame:
    """Convert all amounts to CAD. Creates amount_cad and retains original."""
    df = df.copy()
    df[f"{amount_col}_cad"] = np.where(
        df[currency_col] == "USD",
        (df[amount_col] * USD_TO_CAD_RATE).round(2),
        df[amount_col],
    )
    log.info("Currency normalized → %s (rate: 1 USD = %.4f CAD)", amount_col, USD_TO_CAD_RATE)
    return df


# ── Customer Transformer ───────────────────────────────────────────────────────

def transform_customers(df: pd.DataFrame, environment: str = "dev") -> pd.DataFrame:
    log.info("Transforming customers (%d rows)…", len(df))
    df = df.copy()

    # Enforce dtypes
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")

    # Derived: customer age
    today = pd.Timestamp(date.today())
    df["age_years"] = ((today - df["date_of_birth"]).dt.days / 365.25).round(0).astype("Int64")

    # Derived: tenure in years
    df["tenure_years"] = ((today - df["join_date"]).dt.days / 365.25).round(1)

    # Derived: age band (for segmentation)
    df["age_band"] = pd.cut(
        df["age_years"],
        bins=[0, 25, 35, 50, 65, 120],
        labels=["18-25", "26-35", "36-50", "51-65", "65+"],
        right=True,
    ).astype(str)

    # Null handling: kyc_status default
    df["kyc_status"] = df["kyc_status"].fillna("Unknown")

    # Standardize province codes
    df["province"] = df["province"].str.upper().str.strip()

    # PII masking
    pii_cols = ["first_name", "last_name", "email", "phone", "postal_code"]
    df = apply_pii_masking(df, pii_cols, environment)

    # Audit columns
    df["_transformed_at"] = datetime.utcnow().isoformat()
    df["_pipeline_version"] = "1.0.0"

    # Drop source metadata columns before loading
    df = df.drop(columns=["_source_system", "_extract_ts"], errors="ignore")

    out = PROCESSED_DIR / "customers_staged.parquet"
    df.to_parquet(out, index=False)
    log.info("Customers staged → %s (%d rows)", out, len(df))
    return df


# ── Account Transformer ────────────────────────────────────────────────────────

def transform_accounts(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming accounts (%d rows)…", len(df))
    df = df.copy()

    # Enforce dtypes
    df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce")
    df["last_activity_date"] = pd.to_datetime(df["last_activity_date"], errors="coerce")

    today = pd.Timestamp(date.today())

    # Derived: account age in days
    df["account_age_days"] = (today - df["open_date"]).dt.days.astype("Int64")

    # Derived: days since last activity
    df["days_since_activity"] = (today - df["last_activity_date"]).dt.days.astype("Int64")

    # Derived: dormancy flag (business rule: >730 days inactive = dormant)
    df["is_dormant"] = (
        (df["days_since_activity"] > DORMANCY_THRESHOLD_DAYS) &
        (df["account_status"] == "Active")
    )

    # Derived: balance tier
    df["balance_tier"] = pd.cut(
        df["current_balance"],
        bins=[-np.inf, 0, 1_000, 10_000, 50_000, 100_000, np.inf],
        labels=["Negative", "Under $1K", "$1K–$10K", "$10K–$50K", "$50K–$100K", "Over $100K"],
        right=True,
    ).astype(str)

    # Derived: overdraft flag
    df["is_overdraft"] = df["current_balance"] < 0

    # Null imputation: available_balance defaults to current_balance if null
    df["available_balance"] = df["available_balance"].fillna(df["current_balance"])

    df["_transformed_at"] = datetime.utcnow().isoformat()
    df["_pipeline_version"] = "1.0.0"
    df = df.drop(columns=["_source_system", "_extract_ts"], errors="ignore")

    out = PROCESSED_DIR / "accounts_staged.parquet"
    df.to_parquet(out, index=False)
    log.info("Accounts staged → %s (%d rows)", out, len(df))
    return df


# ── Transaction Transformer ────────────────────────────────────────────────────

def transform_transactions(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming transactions (%d rows)…", len(df))
    df = df.copy()

    # Enforce dtypes
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Deduplication on transaction_id (keep first occurrence)
    dupes = df.duplicated(subset=["transaction_id"], keep="first")
    if dupes.any():
        log.warning("Dropping %d duplicate transactions", dupes.sum())
        df = df[~dupes].copy()

    # Remove null amounts (already quarantined; drop remaining)
    null_amount = df["amount"].isna()
    if null_amount.any():
        log.warning("Dropping %d rows with null amounts", null_amount.sum())
        df = df[~null_amount].copy()

    # Currency normalization
    df = normalize_currency(df, "amount", "currency")

    # Derived: date parts for partitioning and dimensional model
    df["txn_year"] = df["transaction_date"].dt.year.astype("Int64")
    df["txn_month"] = df["transaction_date"].dt.month.astype("Int64")
    df["txn_quarter"] = df["transaction_date"].dt.quarter.astype("Int64")
    df["txn_day_of_week"] = df["transaction_date"].dt.day_name()
    df["txn_week"] = df["transaction_date"].dt.isocalendar().week.astype("Int64")
    df["is_weekend"] = df["txn_day_of_week"].isin(["Saturday", "Sunday"])

    # Derived: transaction direction (signed amount)
    df["signed_amount_cad"] = np.where(
        df["transaction_type"].isin(["Debit", "ATM Withdrawal", "Bill Payment"]),
        -df["amount_cad"],
        df["amount_cad"],
    )

    # Derived: high-value transaction flag (>$10,000 — FINTRAC reporting threshold)
    df["is_large_transaction"] = df["amount_cad"] >= 10_000

    # Merchant category null → "Unclassified"
    df["merchant_category"] = df["merchant_category"].fillna("Unclassified")

    df["_transformed_at"] = datetime.utcnow().isoformat()
    df["_pipeline_version"] = "1.0.0"
    df = df.drop(columns=["_source_system", "_extract_ts"], errors="ignore")

    out = PROCESSED_DIR / "transactions_staged.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    log.info("Transactions staged → %s (%d rows)", out, len(df))
    return df


# ── Credit Transformer ─────────────────────────────────────────────────────────

def transform_credit(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming credit data (%d rows)…", len(df))
    df = df.copy()

    df["origination_date"] = pd.to_datetime(df["origination_date"], errors="coerce")
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")

    today = pd.Timestamp(date.today())

    # Derived: loan age in months
    df["loan_age_months"] = ((today - df["origination_date"]).dt.days / 30.44).round(0).astype("Int64")

    # Derived: months to maturity
    df["months_to_maturity"] = ((df["maturity_date"] - today).dt.days / 30.44).round(0).astype("Int64")

    # Derived: LTV proxy (outstanding / original)
    df["ltv_ratio"] = (df["outstanding_balance"] / df["original_amount"].replace(0, np.nan)).round(4)

    # Derived: credit score band
    df["credit_score_band"] = pd.cut(
        df["credit_score"],
        bins=[0, 579, 669, 739, 799, 900],
        labels=["Poor (<580)", "Fair (580-669)", "Good (670-739)", "Very Good (740-799)", "Excellent (800+)"],
        right=True,
    ).astype(str)

    # Derived: delinquency flag
    df["is_delinquent"] = df["days_past_due"] > 0
    df["delinquency_bucket"] = pd.cut(
        df["days_past_due"],
        bins=[-1, 0, 30, 60, 90, 180, np.inf],
        labels=["Current", "1-30 DPD", "31-60 DPD", "61-90 DPD", "91-180 DPD", "180+ DPD"],
        right=True,
    ).astype(str)

    df["_transformed_at"] = datetime.utcnow().isoformat()
    df["_pipeline_version"] = "1.0.0"
    df = df.drop(columns=["_source_system", "_extract_ts"], errors="ignore")

    out = PROCESSED_DIR / "credit_staged.parquet"
    df.to_parquet(out, index=False)
    log.info("Credit data staged → %s (%d rows)", out, len(df))
    return df
