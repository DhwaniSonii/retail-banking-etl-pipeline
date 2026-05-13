"""
Unit Tests — ETL Transformers

Tests business logic in the transformation layer:
  - PII masking
  - Currency normalization
  - Derived field calculations
  - Null imputation strategies
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parents[1]))

from etl.transformers.transform import (
    mask_pii,
    apply_pii_masking,
    normalize_currency,
    transform_accounts,
    transform_transactions,
    transform_credit,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_accounts():
    return pd.DataFrame({
        "account_id":          ["ACC00000001", "ACC00000002"],
        "customer_id":         ["CUST0000001", "CUST0000002"],
        "account_type":        ["Chequing",    "Savings"],
        "account_status":      ["Active",      "Active"],
        "currency":            ["CAD",         "CAD"],
        "current_balance":     [5000.00,       -200.00],
        "available_balance":   [4900.00,       None],
        "interest_rate":       [0.50,          2.10],
        "open_date":           ["2015-01-01",  "2020-06-15"],
        "last_activity_date":  ["2024-01-01",  "2020-01-01"],  # second one = dormant
        "branch_id":           ["BR0001",      "BR0002"],
        "_source_system":      ["CBS",         "CBS"],
        "_extract_ts":         ["2024-01-01",  "2024-01-01"],
    })


@pytest.fixture
def sample_transactions():
    return pd.DataFrame({
        "transaction_id":    ["TXN0000000001", "TXN0000000001", "TXN0000000002"],  # dup
        "account_id":        ["ACC00000001",   "ACC00000001",   "ACC00000002"],
        "transaction_type":  ["Debit",         "Debit",         "Credit"],
        "channel":           ["Online",        "Online",        "Mobile"],
        "amount":            [150.00,          150.00,          9000.00],
        "currency":          ["CAD",           "CAD",           "USD"],
        "merchant_category": ["Grocery",       "Grocery",       None],
        "transaction_date":  ["2024-01-15",    "2024-01-15",    "2024-02-20"],
        "transaction_time":  ["10:30:00",      "10:30:00",      "14:22:00"],
        "status":            ["Completed",     "Completed",     "Completed"],
        "description":       ["Coffee",        "Coffee",        "Salary"],
        "_source_system":    ["TPS",           "TPS",           "TPS"],
        "_extract_ts":       ["2024-01-15",    "2024-01-15",    "2024-02-20"],
    })


@pytest.fixture
def sample_credit():
    return pd.DataFrame({
        "loan_id":             ["LN00000001",  "LN00000002"],
        "customer_id":         ["CUST0000001", "CUST0000002"],
        "loan_type":           ["Mortgage",    "Personal"],
        "original_amount":     [500000.00,     20000.00],
        "outstanding_balance": [400000.00,     0.00],
        "interest_rate":       [4.50,          12.99],
        "term_months":         [300,           60],
        "origination_date":    ["2020-01-01",  "2022-06-01"],
        "maturity_date":       ["2045-01-01",  "2027-06-01"],
        "credit_score":        [780,           610],
        "risk_rating":         ["AA",          "BB"],
        "days_past_due":       [0,             35],
        "loan_status":         ["Current",     "Delinquent"],
        "collateral_type":     ["Property",    "None"],
        "_source_system":      ["CRS",         "CRS"],
        "_extract_ts":         ["2024-01-01",  "2024-01-01"],
    })


# ── Tests: PII Masking ─────────────────────────────────────────────────────────

class TestPIIMasking:

    def test_mask_pii_returns_string(self):
        result = mask_pii("John")
        assert isinstance(result, str)
        assert result.startswith("MASKED_")

    def test_mask_pii_is_deterministic(self):
        """Same input must always produce same masked output (needed for joins)."""
        assert mask_pii("John Smith") == mask_pii("John Smith")

    def test_mask_pii_different_inputs_differ(self):
        assert mask_pii("John") != mask_pii("Jane")

    def test_mask_pii_handles_null(self):
        assert mask_pii(None) is None

    def test_masking_applied_in_dev(self):
        df = pd.DataFrame({
            "customer_id": ["CUST001"],
            "first_name":  ["Dhwani"],
            "last_name":   ["Soni"],
        })
        result = apply_pii_masking(df, ["first_name", "last_name"], environment="dev")
        assert result["first_name"].iloc[0].startswith("MASKED_")
        assert result["last_name"].iloc[0].startswith("MASKED_")

    def test_masking_skipped_in_prod(self):
        df = pd.DataFrame({
            "customer_id": ["CUST001"],
            "first_name":  ["Dhwani"],
        })
        result = apply_pii_masking(df, ["first_name"], environment="prod")
        assert result["first_name"].iloc[0] == "Dhwani"

    def test_masking_skips_missing_columns(self):
        df = pd.DataFrame({"customer_id": ["CUST001"]})
        # Should not raise even if column doesn't exist
        result = apply_pii_masking(df, ["nonexistent"], environment="dev")
        assert "nonexistent" not in result.columns


# ── Tests: Currency Normalization ──────────────────────────────────────────────

class TestCurrencyNormalization:

    def test_usd_converted_to_cad(self):
        df = pd.DataFrame({
            "amount":   [100.00],
            "currency": ["USD"],
        })
        result = normalize_currency(df, "amount", "currency")
        assert result["amount_cad"].iloc[0] == round(100.00 * 1.36, 2)

    def test_cad_unchanged(self):
        df = pd.DataFrame({
            "amount":   [500.00],
            "currency": ["CAD"],
        })
        result = normalize_currency(df, "amount", "currency")
        assert result["amount_cad"].iloc[0] == 500.00

    def test_mixed_currencies(self):
        df = pd.DataFrame({
            "amount":   [100.00, 200.00],
            "currency": ["USD",  "CAD"],
        })
        result = normalize_currency(df, "amount", "currency")
        assert result["amount_cad"].iloc[0] == round(100 * 1.36, 2)
        assert result["amount_cad"].iloc[1] == 200.00

    def test_original_amount_preserved(self):
        df = pd.DataFrame({"amount": [100.00], "currency": ["USD"]})
        result = normalize_currency(df, "amount", "currency")
        assert result["amount"].iloc[0] == 100.00  # original untouched
        assert "amount_cad" in result.columns


# ── Tests: Account Transformer ─────────────────────────────────────────────────

class TestAccountTransformer:

    def test_is_overdraft_detected(self, sample_accounts, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "etl.transformers.transform.PROCESSED_DIR", tmp_path
        )
        result = transform_accounts(sample_accounts)
        overdraft = result[result["account_id"] == "ACC00000002"]
        assert overdraft["is_overdraft"].iloc[0] is True

    def test_no_overdraft_on_positive_balance(self, sample_accounts, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_accounts(sample_accounts)
        normal = result[result["account_id"] == "ACC00000001"]
        assert normal["is_overdraft"].iloc[0] is False

    def test_available_balance_imputed_from_current(self, sample_accounts, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_accounts(sample_accounts)
        # ACC00000002 had null available_balance → should equal current_balance
        row = result[result["account_id"] == "ACC00000002"]
        assert row["available_balance"].iloc[0] == row["current_balance"].iloc[0]

    def test_source_columns_dropped(self, sample_accounts, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_accounts(sample_accounts)
        assert "_source_system" not in result.columns
        assert "_extract_ts" not in result.columns

    def test_balance_tier_assigned(self, sample_accounts, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_accounts(sample_accounts)
        tiers = result["balance_tier"].tolist()
        assert all(t in ["Negative", "Under $1K", "$1K–$10K", "$10K–$50K", "$50K–$100K", "Over $100K"]
                   for t in tiers)


# ── Tests: Transaction Transformer ────────────────────────────────────────────

class TestTransactionTransformer:

    def test_duplicates_removed(self, sample_transactions, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_transactions(sample_transactions)
        # TXN0000000001 appears twice → should be deduped to once
        assert result["transaction_id"].duplicated().sum() == 0
        assert len(result) == 2

    def test_usd_amount_converted(self, sample_transactions, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_transactions(sample_transactions)
        usd_row = result[result["transaction_id"] == "TXN0000000002"]
        assert usd_row["amount_cad"].iloc[0] == round(9000.00 * 1.36, 2)

    def test_debit_has_negative_signed_amount(self, sample_transactions, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_transactions(sample_transactions)
        debit = result[result["transaction_type"] == "Debit"]
        assert all(debit["signed_amount_cad"] < 0)

    def test_large_transaction_flag(self, sample_transactions, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_transactions(sample_transactions)
        # $9000 USD = $12,240 CAD → should be flagged
        large = result[result["transaction_id"] == "TXN0000000002"]
        assert large["is_large_transaction"].iloc[0] is True

    def test_merchant_null_replaced(self, sample_transactions, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_transactions(sample_transactions)
        assert result["merchant_category"].isna().sum() == 0
        assert "Unclassified" in result["merchant_category"].values

    def test_date_parts_derived(self, sample_transactions, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_transactions(sample_transactions)
        assert "txn_year" in result.columns
        assert "txn_month" in result.columns
        assert "txn_quarter" in result.columns
        assert result["txn_year"].iloc[0] == 2024


# ── Tests: Credit Transformer ──────────────────────────────────────────────────

class TestCreditTransformer:

    def test_delinquency_flag_set(self, sample_credit, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_credit(sample_credit)
        delinquent = result[result["loan_id"] == "LN00000002"]
        assert delinquent["is_delinquent"].iloc[0] is True

    def test_current_loan_not_delinquent(self, sample_credit, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_credit(sample_credit)
        current = result[result["loan_id"] == "LN00000001"]
        assert current["is_delinquent"].iloc[0] is False

    def test_ltv_ratio_calculated(self, sample_credit, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_credit(sample_credit)
        mortgage = result[result["loan_id"] == "LN00000001"]
        expected_ltv = round(400000 / 500000, 4)
        assert mortgage["ltv_ratio"].iloc[0] == expected_ltv

    def test_credit_score_band_assigned(self, sample_credit, tmp_path, monkeypatch):
        monkeypatch.setattr("etl.transformers.transform.PROCESSED_DIR", tmp_path)
        result = transform_credit(sample_credit)
        # Score 780 → Very Good
        high_score = result[result["loan_id"] == "LN00000001"]
        assert "Very Good" in high_score["credit_score_band"].iloc[0]
        # Score 610 → Fair
        low_score = result[result["loan_id"] == "LN00000002"]
        assert "Fair" in low_score["credit_score_band"].iloc[0]
