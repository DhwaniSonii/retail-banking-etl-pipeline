"""
Unit Tests — Data Quality Framework

Tests every check in the QualityCheckRunner to ensure
the DQ framework correctly detects data issues.

Run with:  pytest tests/ -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from etl.transformers.data_quality import (
    QualityCheckRunner,
    TransactionValidator,
    AccountValidator,
    compute_quality_score,
    DataProfiler,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_transactions():
    return pd.DataFrame({
        "transaction_id":   ["TXN0000000001", "TXN0000000002", "TXN0000000003"],
        "account_id":       ["ACC00000001",   "ACC00000002",   "ACC00000001"],
        "transaction_type": ["Debit",         "Credit",        "Transfer"],
        "channel":          ["Online",        "Mobile",        "ATM"],
        "amount":           [150.00,          2500.00,         75.50],
        "currency":         ["CAD",           "CAD",           "USD"],
        "transaction_date": ["2024-01-15",    "2024-02-20",    "2024-03-10"],
        "status":           ["Completed",     "Completed",     "Pending"],
        "merchant_category":["Grocery",       "Salary",        None],
    })


@pytest.fixture
def dirty_transactions():
    return pd.DataFrame({
        "transaction_id":   ["TXN001", "TXN001", "TXN002", None,    "TXN003"],  # dup + null
        "account_id":       ["ACC001", "ACC001", "ACC002", "ACC003","INVALID"],  # orphan
        "transaction_type": ["Debit",  "Debit",  "Credit", "Debit", "Debit"],
        "amount":           [100.00,   100.00,   None,     50.00,   -999999],    # null + outlier
        "currency":         ["CAD",    "CAD",    "CAD",    "CAD",   "CAD"],
        "transaction_date": ["2024-01-01","2024-01-01","2024-01-02","2024-01-03","2024-01-04"],
        "status":           ["Completed","Completed","Completed","Completed","INVALID_STATUS"],
    })


@pytest.fixture
def clean_accounts():
    return pd.DataFrame({
        "account_id":   ["ACC00000001", "ACC00000002", "ACC00000003"],
        "customer_id":  ["CUST0000001", "CUST0000002", "CUST0000001"],
        "account_type": ["Chequing",    "Savings",     "TFSA"],
        "interest_rate":[0.50,          2.10,          3.50],
        "open_date":    ["2020-01-01",  "2019-06-15",  "2021-03-01"],
    })


@pytest.fixture
def runner():
    return QualityCheckRunner()


# ── Tests: Not Null ────────────────────────────────────────────────────────────

class TestNotNull:

    def test_passes_on_clean_data(self, runner, clean_transactions):
        checks = runner.check_not_null(clean_transactions, ["transaction_id", "amount"])
        for check in checks:
            assert check.passed is True
            assert check.failed_count == 0

    def test_detects_null_transaction_id(self, runner, dirty_transactions):
        checks = runner.check_not_null(dirty_transactions, ["transaction_id"])
        txn_check = checks[0]
        assert txn_check.passed is False
        assert txn_check.failed_count == 1
        assert txn_check.dimension == "Completeness"

    def test_detects_null_amount(self, runner, dirty_transactions):
        checks = runner.check_not_null(dirty_transactions, ["amount"])
        assert checks[0].passed is False
        assert checks[0].failed_count == 1

    def test_null_pct_calculated_correctly(self, runner, dirty_transactions):
        checks = runner.check_not_null(dirty_transactions, ["amount"])
        # 1 null out of 5 rows = 20%
        assert checks[0].failed_pct == 20.0

    def test_skips_missing_column_gracefully(self, runner, clean_transactions):
        checks = runner.check_not_null(clean_transactions, ["nonexistent_column"])
        assert len(checks) == 0


# ── Tests: Uniqueness ──────────────────────────────────────────────────────────

class TestUniqueness:

    def test_passes_on_unique_data(self, runner, clean_transactions):
        checks = runner.check_uniqueness(clean_transactions, ["transaction_id"])
        assert checks[0].passed is True
        assert checks[0].failed_count == 0

    def test_detects_duplicate_ids(self, runner, dirty_transactions):
        checks = runner.check_uniqueness(dirty_transactions, ["transaction_id"])
        assert checks[0].passed is False
        assert checks[0].failed_count == 1  # second TXN001 is the duplicate
        assert checks[0].dimension == "Uniqueness"

    def test_duplicate_pct_correct(self, runner, dirty_transactions):
        checks = runner.check_uniqueness(dirty_transactions, ["transaction_id"])
        # 1 duplicate out of 5 rows = 20%
        assert checks[0].failed_pct == 20.0


# ── Tests: Range Validation ────────────────────────────────────────────────────

class TestRangeValidation:

    def test_passes_on_valid_amounts(self, runner, clean_transactions):
        check = runner.check_range(clean_transactions, "amount", min_val=0, max_val=500_000)
        assert check.passed is True

    def test_detects_negative_amount(self, runner, dirty_transactions):
        check = runner.check_range(dirty_transactions, "amount", min_val=0, max_val=500_000)
        assert check.passed is False
        assert check.failed_count >= 1
        assert check.dimension == "Validity"

    def test_only_min_bound(self, runner):
        df = pd.DataFrame({"amount": [10, 20, -5, 0]})
        check = runner.check_range(df, "amount", min_val=0)
        assert check.passed is False
        assert check.failed_count == 1  # only -5

    def test_only_max_bound(self, runner):
        df = pd.DataFrame({"amount": [10, 20, 600_000]})
        check = runner.check_range(df, "amount", max_val=500_000)
        assert check.passed is False
        assert check.failed_count == 1

    def test_ignores_nulls_in_range_check(self, runner):
        df = pd.DataFrame({"amount": [10.0, None, 20.0]})
        check = runner.check_range(df, "amount", min_val=0, max_val=100)
        assert check.passed is True  # nulls should not be flagged as range violations


# ── Tests: Allowed Values ──────────────────────────────────────────────────────

class TestAllowedValues:

    def test_passes_on_valid_status(self, runner, clean_transactions):
        check = runner.check_allowed_values(
            clean_transactions, "status",
            {"Completed", "Pending", "Failed", "Reversed"}
        )
        assert check.passed is True

    def test_detects_invalid_status(self, runner, dirty_transactions):
        check = runner.check_allowed_values(
            dirty_transactions, "status",
            {"Completed", "Pending", "Failed", "Reversed"}
        )
        assert check.passed is False
        assert check.failed_count == 1  # "INVALID_STATUS"

    def test_returns_none_for_missing_column(self, runner, clean_transactions):
        result = runner.check_allowed_values(clean_transactions, "no_such_col", {"A", "B"})
        assert result is None


# ── Tests: Referential Integrity ───────────────────────────────────────────────

class TestReferentialIntegrity:

    def test_passes_when_all_fks_valid(self, runner, clean_transactions):
        valid_accounts = {"ACC00000001", "ACC00000002"}
        check = runner.check_referential_integrity(clean_transactions, "account_id", valid_accounts)
        assert check.passed is True

    def test_detects_orphan_foreign_key(self, runner, dirty_transactions):
        valid_accounts = {"ACC001", "ACC002", "ACC003"}
        check = runner.check_referential_integrity(dirty_transactions, "account_id", valid_accounts)
        assert check.passed is False
        assert check.failed_count == 1  # "INVALID" is the orphan
        assert check.dimension == "Referential Integrity"


# ── Tests: Quality Score ───────────────────────────────────────────────────────

class TestQualityScore:

    def test_perfect_score_all_pass(self, runner, clean_transactions):
        checks = runner.check_not_null(clean_transactions, ["transaction_id", "amount"])
        score = compute_quality_score(checks)
        assert score == 100.0

    def test_score_decreases_on_failure(self, runner, dirty_transactions):
        checks = runner.check_not_null(dirty_transactions, ["transaction_id", "amount"])
        score = compute_quality_score(checks)
        assert score < 100.0

    def test_halt_action_penalizes_more_than_flag(self, runner):
        df_bad = pd.DataFrame({"col": [None, None, None, 1, 2]})
        halt_check = runner.check_not_null(df_bad, ["col"], action="QUARANTINE")
        halt_check[0].action  # QUARANTINE = weight 3

        df_flag = pd.DataFrame({"col": [None, None, None, 1, 2]})
        flag_check = runner.check_not_null(df_flag, ["col"], action="FLAG")

        score_halt = compute_quality_score(halt_check)
        score_flag = compute_quality_score(flag_check)
        assert score_halt <= score_flag

    def test_empty_checks_returns_100(self):
        assert compute_quality_score([]) == 100.0


# ── Tests: TransactionValidator (integration) ──────────────────────────────────

class TestTransactionValidator:

    def test_clean_data_passes_through(self, clean_transactions):
        account_ids = set(clean_transactions["account_id"])
        validator = TransactionValidator(account_ids)
        clean_df, checks = validator.validate(clean_transactions)
        assert len(clean_df) == len(clean_transactions)

    def test_dirty_data_quarantined(self, dirty_transactions):
        account_ids = {"ACC001", "ACC002", "ACC003"}
        validator = TransactionValidator(account_ids)
        clean_df, checks = validator.validate(dirty_transactions)
        # Dirty data should have fewer rows after quarantine
        assert len(clean_df) < len(dirty_transactions)

    def test_all_checks_have_required_fields(self, clean_transactions):
        validator = TransactionValidator(set(clean_transactions["account_id"]))
        _, checks = validator.validate(clean_transactions)
        for check in checks:
            assert check.check_name is not None
            assert check.dimension is not None
            assert check.action in {"QUARANTINE", "FLAG", "WARN", "HALT"}
            assert 0 <= check.failed_pct <= 100


# ── Tests: DataProfiler ────────────────────────────────────────────────────────

class TestDataProfiler:

    def test_profiles_all_columns(self, clean_transactions):
        profiler = DataProfiler()
        profiles = profiler.profile(clean_transactions)
        assert len(profiles) == len(clean_transactions.columns)

    def test_null_count_correct(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "y", None]})
        profiles = profiler.profile(df)
        null_counts = {p.column_name: p.null_count for p in profiles}
        assert null_counts["a"] == 1
        assert null_counts["b"] == 1

    def test_numeric_stats_computed(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"amount": [100.0, 200.0, 300.0]})
        profiles = profiler.profile(df)
        p = profiles[0]
        assert p.min_value == 100.0
        assert p.max_value == 300.0
        assert p.mean_value == 200.0

    def test_unique_count_correct(self, clean_transactions):
        profiler = DataProfiler()
        profiles = profiler.profile(clean_transactions)
        txn_profile = next(p for p in profiles if p.column_name == "transaction_id")
        assert txn_profile.unique_count == 3
        assert txn_profile.unique_pct == 100.0
