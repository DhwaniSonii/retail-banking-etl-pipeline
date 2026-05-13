"""
Data Quality Framework — Banking Data Platform

Runs at every pipeline stage. Produces:
  - Per-dataset quality scorecards
  - Column-level profiling report
  - Quarantine file for failed records
  - JSON quality report written to data/quality_reports/

Quality dimensions tracked:
  Completeness · Uniqueness · Validity · Referential Integrity · Freshness · Consistency
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

log = logging.getLogger("dq.framework")

QUALITY_REPORT_DIR = Path(__file__).parents[2] / "data" / "quality_reports"
QUARANTINE_DIR = Path(__file__).parents[2] / "data" / "quarantine"
QUALITY_REPORT_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

# ── Data Classes ───────────────────────────────────────────────────────────────

@dataclass
class ColumnProfile:
    column_name: str
    dtype: str
    total_count: int
    null_count: int
    null_pct: float
    unique_count: int
    unique_pct: float
    min_value: Any = None
    max_value: Any = None
    mean_value: Any = None
    std_value: Any = None
    sample_values: list = field(default_factory=list)


@dataclass
class QualityCheck:
    check_name: str
    dimension: str          # Completeness / Uniqueness / Validity / Referential / Freshness
    column: str | None
    rule_description: str
    passed: bool
    failed_count: int
    failed_pct: float
    action: str             # QUARANTINE / FLAG / HALT / WARN
    details: str = ""


@dataclass
class DatasetQualityReport:
    dataset_name: str
    run_timestamp: str
    total_rows: int
    total_columns: int
    overall_score: float    # 0-100
    checks: list[QualityCheck] = field(default_factory=list)
    column_profiles: list[ColumnProfile] = field(default_factory=list)
    quarantined_rows: int = 0
    pipeline_action: str = "PROCEED"   # PROCEED / WARN / HALT


# ── Profiler ───────────────────────────────────────────────────────────────────

class DataProfiler:
    """Generates column-level statistics for a dataset."""

    def profile(self, df: pd.DataFrame) -> list[ColumnProfile]:
        profiles = []
        n = len(df)

        for col in df.columns:
            s = df[col]
            null_count = int(s.isna().sum())
            unique_count = int(s.nunique(dropna=True))

            profile = ColumnProfile(
                column_name=col,
                dtype=str(s.dtype),
                total_count=n,
                null_count=null_count,
                null_pct=round(null_count / n * 100, 2) if n else 0,
                unique_count=unique_count,
                unique_pct=round(unique_count / n * 100, 2) if n else 0,
                sample_values=s.dropna().head(5).tolist(),
            )

            if pd.api.types.is_numeric_dtype(s):
                profile.min_value = round(float(s.min()), 4) if not s.isna().all() else None
                profile.max_value = round(float(s.max()), 4) if not s.isna().all() else None
                profile.mean_value = round(float(s.mean()), 4) if not s.isna().all() else None
                profile.std_value = round(float(s.std()), 4) if not s.isna().all() else None

            profiles.append(profile)

        return profiles


# ── Check Library ──────────────────────────────────────────────────────────────

class QualityCheckRunner:
    """Library of reusable quality checks."""

    def check_not_null(self, df: pd.DataFrame, columns: list[str], action: str = "QUARANTINE") -> list[QualityCheck]:
        checks = []
        for col in columns:
            if col not in df.columns:
                continue
            failed = df[col].isna()
            checks.append(QualityCheck(
                check_name=f"not_null_{col}",
                dimension="Completeness",
                column=col,
                rule_description=f"Column '{col}' must not contain null values",
                passed=not failed.any(),
                failed_count=int(failed.sum()),
                failed_pct=round(failed.mean() * 100, 2),
                action=action,
                details=f"Nulls found in {int(failed.sum())} rows" if failed.any() else "OK",
            ))
        return checks

    def check_uniqueness(self, df: pd.DataFrame, columns: list[str], action: str = "QUARANTINE") -> list[QualityCheck]:
        checks = []
        for col in columns:
            if col not in df.columns:
                continue
            dupes = df.duplicated(subset=[col], keep="first")
            checks.append(QualityCheck(
                check_name=f"unique_{col}",
                dimension="Uniqueness",
                column=col,
                rule_description=f"Column '{col}' must contain unique values",
                passed=not dupes.any(),
                failed_count=int(dupes.sum()),
                failed_pct=round(dupes.mean() * 100, 2),
                action=action,
                details=f"{int(dupes.sum())} duplicate rows detected" if dupes.any() else "OK",
            ))
        return checks

    def check_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_val: float | None = None,
        max_val: float | None = None,
        action: str = "FLAG",
    ) -> QualityCheck:
        mask = pd.Series([False] * len(df), index=df.index)
        if min_val is not None:
            mask |= df[column].lt(min_val)
        if max_val is not None:
            mask |= df[column].gt(max_val)
        mask &= df[column].notna()

        return QualityCheck(
            check_name=f"range_{column}",
            dimension="Validity",
            column=column,
            rule_description=f"'{column}' must be between {min_val} and {max_val}",
            passed=not mask.any(),
            failed_count=int(mask.sum()),
            failed_pct=round(mask.mean() * 100, 2),
            action=action,
            details=f"{int(mask.sum())} values outside expected range" if mask.any() else "OK",
        )

    def check_allowed_values(
        self,
        df: pd.DataFrame,
        column: str,
        allowed: set,
        action: str = "FLAG",
    ) -> QualityCheck:
        if column not in df.columns:
            return None
        invalid = ~df[column].isin(allowed) & df[column].notna()
        return QualityCheck(
            check_name=f"allowed_values_{column}",
            dimension="Validity",
            column=column,
            rule_description=f"'{column}' must be one of: {sorted(allowed)}",
            passed=not invalid.any(),
            failed_count=int(invalid.sum()),
            failed_pct=round(invalid.mean() * 100, 2),
            action=action,
            details=f"Invalid values: {df.loc[invalid, column].unique()[:5].tolist()}" if invalid.any() else "OK",
        )

    def check_referential_integrity(
        self,
        df: pd.DataFrame,
        fk_column: str,
        reference_set: set,
        action: str = "QUARANTINE",
    ) -> QualityCheck:
        if fk_column not in df.columns:
            return None
        orphans = ~df[fk_column].isin(reference_set) & df[fk_column].notna()
        return QualityCheck(
            check_name=f"referential_integrity_{fk_column}",
            dimension="Referential Integrity",
            column=fk_column,
            rule_description=f"All '{fk_column}' values must exist in the reference table",
            passed=not orphans.any(),
            failed_count=int(orphans.sum()),
            failed_pct=round(orphans.mean() * 100, 2),
            action=action,
            details=f"Orphan keys: {df.loc[orphans, fk_column].unique()[:5].tolist()}" if orphans.any() else "OK",
        )

    def check_freshness(
        self,
        df: pd.DataFrame,
        date_column: str,
        max_age_days: int,
        action: str = "WARN",
    ) -> QualityCheck:
        if date_column not in df.columns:
            return None
        dates = pd.to_datetime(df[date_column], errors="coerce")
        stale = (datetime.utcnow() - dates).dt.days.gt(max_age_days)
        return QualityCheck(
            check_name=f"freshness_{date_column}",
            dimension="Freshness",
            column=date_column,
            rule_description=f"'{date_column}' values must be within {max_age_days} days",
            passed=not stale.any(),
            failed_count=int(stale.sum()),
            failed_pct=round(stale.mean() * 100, 2),
            action=action,
            details=f"Oldest record: {dates.min().date()}" if not dates.isna().all() else "No valid dates",
        )


# ── Dataset-specific validators ────────────────────────────────────────────────

class TransactionValidator:
    def __init__(self, account_ids: set):
        self.account_ids = account_ids
        self.runner = QualityCheckRunner()

    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[QualityCheck]]:
        checks: list[QualityCheck] = []

        checks += self.runner.check_not_null(df, ["transaction_id", "account_id", "amount", "transaction_date"])
        checks += self.runner.check_uniqueness(df, ["transaction_id"])

        checks.append(self.runner.check_range(df, "amount", min_val=0, max_val=500_000, action="FLAG"))

        ref_check = self.runner.check_referential_integrity(df, "account_id", self.account_ids)
        if ref_check:
            checks.append(ref_check)

        allowed_status = {"Completed", "Pending", "Failed", "Reversed"}
        allowed_type = {"Debit", "Credit", "Transfer", "Bill Payment", "ATM Withdrawal"}
        for chk in [
            self.runner.check_allowed_values(df, "status", allowed_status),
            self.runner.check_allowed_values(df, "transaction_type", allowed_type),
        ]:
            if chk:
                checks.append(chk)

        checks.append(self.runner.check_freshness(df, "transaction_date", max_age_days=730))

        # Quarantine bad rows
        quarantine_mask = pd.Series([False] * len(df), index=df.index)
        for chk in checks:
            if chk.action == "QUARANTINE" and not chk.passed and chk.column:
                if chk.check_name.startswith("not_null"):
                    quarantine_mask |= df[chk.column].isna()
                elif chk.check_name.startswith("unique"):
                    quarantine_mask |= df.duplicated(subset=[chk.column], keep="first")
                elif chk.check_name.startswith("referential"):
                    quarantine_mask |= ~df[chk.column].isin(self.account_ids) & df[chk.column].notna()

        quarantine_df = df[quarantine_mask].copy()
        clean_df = df[~quarantine_mask].copy()

        if len(quarantine_df) > 0:
            q_path = QUARANTINE_DIR / f"transactions_quarantine_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            quarantine_df.to_csv(q_path, index=False)
            log.warning("Quarantined %d transaction rows → %s", len(quarantine_df), q_path)

        return clean_df, checks


class AccountValidator:
    def __init__(self, customer_ids: set):
        self.customer_ids = customer_ids
        self.runner = QualityCheckRunner()

    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[QualityCheck]]:
        checks: list[QualityCheck] = []

        checks += self.runner.check_not_null(df, ["account_id", "customer_id", "account_type", "open_date"])
        checks += self.runner.check_uniqueness(df, ["account_id"])

        ref_check = self.runner.check_referential_integrity(df, "customer_id", self.customer_ids)
        if ref_check:
            checks.append(ref_check)

        checks.append(self.runner.check_allowed_values(
            df, "account_type",
            {"Chequing", "Savings", "TFSA", "RRSP", "GIC"},
        ))
        checks.append(self.runner.check_range(df, "interest_rate", min_val=0, max_val=25))

        return df, checks


# ── Report Generator ───────────────────────────────────────────────────────────

def compute_quality_score(checks: list[QualityCheck]) -> float:
    """
    Weighted score: QUARANTINE failures penalize heavily, FLAG lightly.
    """
    if not checks:
        return 100.0

    weights = {"QUARANTINE": 3.0, "HALT": 5.0, "FLAG": 1.0, "WARN": 0.5}
    total_weight = sum(weights.get(c.action, 1.0) for c in checks)
    penalty = sum(
        weights.get(c.action, 1.0) * (c.failed_pct / 100)
        for c in checks if not c.passed
    )
    return round(max(0.0, 100.0 - (penalty / total_weight * 100)), 1)


def generate_report(
    dataset_name: str,
    df: pd.DataFrame,
    checks: list[QualityCheck],
    quarantined_rows: int = 0,
) -> DatasetQualityReport:
    profiler = DataProfiler()
    profiles = profiler.profile(df)
    score = compute_quality_score(checks)

    failed_halt = any(c.action == "HALT" and not c.passed for c in checks)
    pipeline_action = "HALT" if failed_halt else ("WARN" if score < 85 else "PROCEED")

    report = DatasetQualityReport(
        dataset_name=dataset_name,
        run_timestamp=datetime.utcnow().isoformat(),
        total_rows=len(df),
        total_columns=len(df.columns),
        overall_score=score,
        checks=checks,
        column_profiles=profiles,
        quarantined_rows=quarantined_rows,
        pipeline_action=pipeline_action,
    )

    # Write JSON report
    report_path = QUALITY_REPORT_DIR / f"{dataset_name}_dq_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w") as f:
        json.dump(asdict(report), f, indent=2, default=str)

    log.info(
        "DQ Report [%s] → Score: %.1f | Action: %s | Quarantined: %d | Path: %s",
        dataset_name, score, pipeline_action, quarantined_rows, report_path,
    )

    _print_summary(report)
    return report


def _print_summary(report: DatasetQualityReport) -> None:
    print(f"\n{'═'*65}")
    print(f"  DATA QUALITY REPORT — {report.dataset_name.upper()}")
    print(f"{'═'*65}")
    print(f"  Rows          : {report.total_rows:,}")
    print(f"  Quality Score : {report.overall_score:.1f} / 100")
    print(f"  Pipeline      : {report.pipeline_action}")
    print(f"  Quarantined   : {report.quarantined_rows:,} rows")
    print(f"\n  {'Check':<40} {'Passed':<8} {'Failed':>8}  {'Action'}")
    print(f"  {'-'*70}")
    for chk in report.checks:
        status = "✓" if chk.passed else "✗"
        print(f"  {status} {chk.check_name:<38} {'Yes' if chk.passed else 'No':<8} {chk.failed_count:>8}  {chk.action}")
    print(f"{'═'*65}\n")
