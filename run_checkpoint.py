"""
Great Expectations Integration — Banking Data Platform

Industry-standard data validation framework used at major banks.
Defines expectation suites for each dataset and a checkpoint
that runs as part of the pipeline.

Run standalone:
    python great_expectations/run_checkpoint.py

Or import and call run_ge_checkpoint() from the pipeline runner.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))

log = logging.getLogger("great_expectations.banking")

# ─────────────────────────────────────────────────────────────────────────────
# NOTE: Great Expectations uses a project context (great_expectations/ folder).
# This file uses the fluent API (GX Core 0.18+) which works without a full
# GX project structure — ideal for portfolio / CI use.
# ─────────────────────────────────────────────────────────────────────────────

def build_transaction_suite(df: pd.DataFrame):
    """
    Define expectation suite for transaction data.
    Returns a GX ValidationResult.
    """
    try:
        import great_expectations as gx
    except ImportError:
        log.error("great_expectations not installed. Run: pip install great-expectations")
        return None

    context = gx.get_context(mode="ephemeral")

    data_source = context.sources.add_pandas("transactions_source")
    asset = data_source.add_dataframe_asset("transactions_raw")
    batch_request = asset.build_batch_request(dataframe=df)

    suite = context.add_expectation_suite("transactions_suite")

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    # ── Completeness ──────────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_not_be_null("account_id")
    validator.expect_column_values_to_not_be_null("amount")
    validator.expect_column_values_to_not_be_null("transaction_date")
    validator.expect_column_values_to_not_be_null("status")

    # ── Uniqueness ────────────────────────────────────────────────────────────
    validator.expect_column_values_to_be_unique("transaction_id")

    # ── Validity — allowed values ─────────────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "status", ["Completed", "Pending", "Failed", "Reversed"]
    )
    validator.expect_column_values_to_be_in_set(
        "transaction_type",
        ["Debit", "Credit", "Transfer", "Bill Payment", "ATM Withdrawal"]
    )
    validator.expect_column_values_to_be_in_set(
        "currency", ["CAD", "USD"]
    )

    # ── Validity — range ──────────────────────────────────────────────────────
    validator.expect_column_values_to_be_between(
        "amount", min_value=0, max_value=500_000,
        mostly=0.999,  # allow 0.1% edge cases
    )

    # ── Schema ────────────────────────────────────────────────────────────────
    validator.expect_column_to_exist("transaction_id")
    validator.expect_column_to_exist("account_id")
    validator.expect_column_to_exist("amount")
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10_000_000)

    # ── Statistical expectations ──────────────────────────────────────────────
    validator.expect_column_mean_to_be_between("amount", min_value=50, max_value=5_000)
    validator.expect_column_median_to_be_between("amount", min_value=10, max_value=1_000)

    results = validator.validate()
    return results


def build_account_suite(df: pd.DataFrame):
    try:
        import great_expectations as gx
    except ImportError:
        return None

    context = gx.get_context(mode="ephemeral")
    data_source = context.sources.add_pandas("accounts_source")
    asset = data_source.add_dataframe_asset("accounts_raw")
    batch_request = asset.build_batch_request(dataframe=df)

    suite = context.add_expectation_suite("accounts_suite")
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    validator.expect_column_values_to_not_be_null("account_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("account_id")

    validator.expect_column_values_to_be_in_set(
        "account_type", ["Chequing", "Savings", "TFSA", "RRSP", "GIC"]
    )
    validator.expect_column_values_to_be_in_set(
        "account_status", ["Active", "Dormant", "Closed", "Frozen"]
    )
    validator.expect_column_values_to_be_between(
        "interest_rate", min_value=0, max_value=25
    )

    return validator.validate()


def run_ge_checkpoint() -> dict:
    """
    Run GE checkpoints on all staged datasets.
    Returns a summary dict suitable for logging or CI assertions.
    """
    from pathlib import Path
    raw_dir = Path(__file__).parents[1] / "data" / "raw"

    results_summary = {}

    datasets_to_validate = {
        "transactions": (raw_dir / "transactions_raw.csv", build_transaction_suite),
        "accounts":     (raw_dir / "accounts_raw.csv",     build_account_suite),
    }

    for name, (path, suite_fn) in datasets_to_validate.items():
        if not path.exists():
            log.warning("Raw file not found for GE validation: %s", path)
            results_summary[name] = {"status": "skipped", "success": None}
            continue

        df = pd.read_csv(path)
        log.info("Running GE expectations on %s (%d rows)…", name, len(df))

        results = suite_fn(df)
        if results is None:
            results_summary[name] = {"status": "skipped — GE not installed", "success": None}
            continue

        success = results.success
        n_pass = sum(1 for r in results.results if r.success)
        n_fail = sum(1 for r in results.results if not r.success)

        results_summary[name] = {
            "status": "passed" if success else "failed",
            "success": success,
            "checks_passed": n_pass,
            "checks_failed": n_fail,
        }

        log.info(
            "GE [%s] → %s | Passed: %d | Failed: %d",
            name, "✓" if success else "✗", n_pass, n_fail,
        )

        if not success:
            failed = [r for r in results.results if not r.success]
            for r in failed[:5]:  # log first 5 failures
                log.warning("  FAILED: %s", r.expectation_config.expectation_type)

    return results_summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    summary = run_ge_checkpoint()
    print("\nGreat Expectations Summary:")
    for dataset, result in summary.items():
        print(f"  {dataset:<20}: {result['status']}")
