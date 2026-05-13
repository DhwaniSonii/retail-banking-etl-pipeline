"""
Banking Data Platform — Pipeline Runner

Entry point for running the full ETL pipeline locally
without Airflow (useful for development and testing).

Usage:
    python etl/run_pipeline.py
    python etl/run_pipeline.py --stage extract
    python etl/run_pipeline.py --stage transform
    python etl/run_pipeline.py --stage validate
    python etl/run_pipeline.py --env prod
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parents[1]))

from etl.extractors.generate_banking_data import run_extraction
from etl.transformers.transform import (
    transform_customers,
    transform_accounts,
    transform_transactions,
    transform_credit,
)
from etl.transformers.data_quality import (
    TransactionValidator,
    AccountValidator,
    generate_report,
)
from governance.lineage.lineage_graph import LineageGraph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger("pipeline.runner")

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"


def run_extract() -> dict[str, pd.DataFrame]:
    log.info("━" * 60)
    log.info("STAGE 1 — EXTRACT")
    log.info("━" * 60)
    return run_extraction()


def run_validate(datasets: dict[str, pd.DataFrame]) -> bool:
    log.info("━" * 60)
    log.info("STAGE 2 — DATA QUALITY VALIDATION")
    log.info("━" * 60)

    all_passed = True

    # Validate transactions
    txn_validator = TransactionValidator(
        account_ids=set(datasets["accounts"]["account_id"])
    )
    clean_txn, txn_checks = txn_validator.validate(datasets["transactions"])
    txn_report = generate_report(
        "transactions_raw",
        clean_txn,
        txn_checks,
        quarantined_rows=len(datasets["transactions"]) - len(clean_txn),
    )
    if txn_report.pipeline_action == "HALT":
        log.error("PIPELINE HALT: transactions quality score %.1f is below threshold", txn_report.overall_score)
        all_passed = False

    # Validate accounts
    acct_validator = AccountValidator(
        customer_ids=set(datasets["customers"]["customer_id"])
    )
    _, acct_checks = acct_validator.validate(datasets["accounts"])
    acct_report = generate_report("accounts_raw", datasets["accounts"], acct_checks)
    if acct_report.pipeline_action == "HALT":
        log.error("PIPELINE HALT: accounts quality score %.1f is below threshold", acct_report.overall_score)
        all_passed = False

    return all_passed


def run_transform(environment: str = "dev") -> None:
    log.info("━" * 60)
    log.info("STAGE 3 — TRANSFORM")
    log.info("━" * 60)

    customers_raw = pd.read_csv(RAW_DIR / "customers_raw.csv")
    accounts_raw = pd.read_csv(RAW_DIR / "accounts_raw.csv")
    transactions_raw = pd.read_csv(RAW_DIR / "transactions_raw.csv")
    credit_raw = pd.read_csv(RAW_DIR / "credit_data_raw.csv")

    transform_customers(customers_raw, environment=environment)
    transform_accounts(accounts_raw)
    transform_transactions(transactions_raw)
    transform_credit(credit_raw)

    log.info("All transforms complete. Staged files written to %s", PROCESSED_DIR)


def run_governance() -> None:
    log.info("━" * 60)
    log.info("STAGE 4 — GOVERNANCE & LINEAGE")
    log.info("━" * 60)

    graph = LineageGraph()
    docs_dir = Path(__file__).parents[1] / "docs"
    docs_dir.mkdir(exist_ok=True)

    graph.export_json(Path(__file__).parents[1] / "governance" / "lineage" / "lineage_graph.json")
    graph.export_markdown(docs_dir / "DATA_LINEAGE.md")

    # Print a sample lineage trace
    graph.print_lineage("metrics.kpi_daily_summary", "total_volume_cad")


def print_summary(start_time: float) -> None:
    elapsed = time.time() - start_time
    log.info("━" * 60)
    log.info("PIPELINE COMPLETE in %.1f seconds", elapsed)
    log.info("━" * 60)

    processed = list(PROCESSED_DIR.glob("*.parquet"))
    quality_reports = list((Path(__file__).parents[1] / "data" / "quality_reports").glob("*.json"))

    print(f"\n{'═'*65}")
    print("  BANKING DATA PLATFORM — PIPELINE SUMMARY")
    print(f"{'═'*65}")
    print(f"  Elapsed time     : {elapsed:.1f}s")
    print(f"  Staged files     : {len(processed)}")
    for f in processed:
        df = pd.read_parquet(f)
        print(f"    ✓ {f.name:<40} {len(df):>8,} rows")
    print(f"  Quality reports  : {len(quality_reports)}")
    print(f"  Lineage edges    : {len(__import__('governance.lineage.lineage_graph', fromlist=['LINEAGE_EDGES']).LINEAGE_EDGES)}")
    print(f"{'═'*65}\n")


def main(stage: str = "all", environment: str = "dev") -> None:
    start = time.time()
    log.info("Banking Data Platform — Starting pipeline (stage=%s, env=%s)", stage, environment)

    datasets = {}

    if stage in ("all", "extract"):
        datasets = run_extract()

    if stage in ("all", "validate"):
        if not datasets:
            datasets = {
                "customers":    pd.read_csv(RAW_DIR / "customers_raw.csv"),
                "accounts":     pd.read_csv(RAW_DIR / "accounts_raw.csv"),
                "transactions": pd.read_csv(RAW_DIR / "transactions_raw.csv"),
                "credit":       pd.read_csv(RAW_DIR / "credit_data_raw.csv"),
            }
        quality_ok = run_validate(datasets)
        if not quality_ok and stage == "all":
            log.error("Pipeline halted due to data quality failures.")
            sys.exit(1)

    if stage in ("all", "transform"):
        run_transform(environment=environment)

    if stage in ("all", "governance"):
        run_governance()

    if stage == "all":
        print_summary(start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Banking Data Platform — Pipeline Runner")
    parser.add_argument(
        "--stage",
        choices=["all", "extract", "validate", "transform", "governance"],
        default="all",
        help="Pipeline stage to run (default: all)",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment for PII masking (dev = masked, prod = unmasked)",
    )
    args = parser.parse_args()
    main(stage=args.stage, environment=args.env)
