"""
Data Lineage Tracker — Column-Level Lineage

Tracks the full journey of every data field from upstream source
systems through staging, dimensional models, and into KPI metrics.

Lineage graph format:
  source_system.table.column → staging.table.column → marts.table.column → metrics.table.column

This file:
  1. Defines the lineage registry (static metadata)
  2. Provides a LineageGraph class to query ancestry / descendants
  3. Can export lineage to JSON, Markdown, or the governance DB
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Generator

LINEAGE_DIR = Path(__file__).parent
LINEAGE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class LineageEdge:
    source_dataset: str
    source_column: str
    target_dataset: str
    target_column: str
    transformation_rule: str
    pipeline_step: str


# ── Lineage Registry ───────────────────────────────────────────────────────────
# Maps every column's journey through the pipeline.
# Format: (source_dataset, source_col, target_dataset, target_col, rule, step)

LINEAGE_EDGES: list[LineageEdge] = [

    # ── Customer lineage ──────────────────────────────────────────────────────

    LineageEdge("CORE_BANKING.customers_raw",  "customer_id",      "staging.stg_customers",    "customer_id",       "Direct copy",                          "extractor"),
    LineageEdge("CORE_BANKING.customers_raw",  "first_name",       "staging.stg_customers",    "first_name",        "SHA-256 mask in non-prod",              "transformer.pii_masking"),
    LineageEdge("CORE_BANKING.customers_raw",  "last_name",        "staging.stg_customers",    "last_name",         "SHA-256 mask in non-prod",              "transformer.pii_masking"),
    LineageEdge("CORE_BANKING.customers_raw",  "email",            "staging.stg_customers",    "email",             "SHA-256 mask in non-prod",              "transformer.pii_masking"),
    LineageEdge("CORE_BANKING.customers_raw",  "date_of_birth",    "staging.stg_customers",    "date_of_birth",     "Cast to DATE",                          "transformer"),
    LineageEdge("CORE_BANKING.customers_raw",  "date_of_birth",    "staging.stg_customers",    "age_years",         "Derived: (today - DOB).days / 365.25",  "transformer.derive"),
    LineageEdge("CORE_BANKING.customers_raw",  "join_date",        "staging.stg_customers",    "tenure_years",      "Derived: (today - join_date).days / 365.25", "transformer.derive"),
    LineageEdge("CORE_BANKING.customers_raw",  "date_of_birth",    "staging.stg_customers",    "age_band",          "Derived: pd.cut on age_years",          "transformer.derive"),
    LineageEdge("staging.stg_customers",       "customer_id",      "marts.dim_customer",        "customer_id",       "Direct copy with surrogate key added",  "dbt.dim_customer"),
    LineageEdge("staging.stg_customers",       "customer_segment", "marts.dim_customer",        "customer_segment",  "Direct copy",                           "dbt.dim_customer"),
    LineageEdge("staging.stg_customers",       "age_band",         "marts.dim_customer",        "age_band",          "Direct copy",                           "dbt.dim_customer"),
    LineageEdge("marts.dim_customer",          "customer_sk",      "marts.fact_transactions",   "customer_sk",       "FK join on account_id → customer_id",   "dbt.fact_transactions"),

    # ── Account lineage ───────────────────────────────────────────────────────

    LineageEdge("CORE_BANKING.accounts_raw",   "account_id",       "staging.stg_accounts",     "account_id",        "Direct copy",                           "extractor"),
    LineageEdge("CORE_BANKING.accounts_raw",   "current_balance",  "staging.stg_accounts",     "current_balance",   "Cast to NUMERIC(18,2)",                 "transformer"),
    LineageEdge("CORE_BANKING.accounts_raw",   "current_balance",  "staging.stg_accounts",     "balance_tier",      "Derived: pd.cut on balance",            "transformer.derive"),
    LineageEdge("CORE_BANKING.accounts_raw",   "last_activity_date","staging.stg_accounts",    "days_since_activity","Derived: (today - last_activity).days", "transformer.derive"),
    LineageEdge("CORE_BANKING.accounts_raw",   "last_activity_date","staging.stg_accounts",    "is_dormant",        "Derived: days_since_activity > 730",    "transformer.derive"),
    LineageEdge("CORE_BANKING.accounts_raw",   "current_balance",  "staging.stg_accounts",     "is_overdraft",      "Derived: balance < 0",                  "transformer.derive"),
    LineageEdge("staging.stg_accounts",        "account_id",       "marts.dim_account",         "account_id",        "Direct copy with surrogate key",        "dbt.dim_account"),
    LineageEdge("staging.stg_accounts",        "balance_tier",     "marts.dim_account",         "balance_tier",      "Direct copy",                           "dbt.dim_account"),
    LineageEdge("staging.stg_accounts",        "is_dormant",       "marts.dim_account",         "is_dormant",        "Direct copy",                           "dbt.dim_account"),
    LineageEdge("marts.dim_account",           "account_sk",       "marts.fact_transactions",   "account_sk",        "FK join on transaction.account_id",     "dbt.fact_transactions"),

    # ── Transaction lineage ───────────────────────────────────────────────────

    LineageEdge("TPS.transactions_raw",        "transaction_id",   "staging.stg_transactions", "transaction_id",    "Direct copy after dedup",               "transformer.dedup"),
    LineageEdge("TPS.transactions_raw",        "amount",           "staging.stg_transactions", "amount",            "Direct copy (original currency)",        "transformer"),
    LineageEdge("TPS.transactions_raw",        "amount",           "staging.stg_transactions", "amount_cad",        "Derived: USD×1.36 if USD else amount",   "transformer.currency_normalize"),
    LineageEdge("TPS.transactions_raw",        "amount",           "staging.stg_transactions", "signed_amount_cad", "Derived: negate if Debit/ATM/BillPay",  "transformer.derive"),
    LineageEdge("TPS.transactions_raw",        "amount",           "staging.stg_transactions", "is_large_transaction","Derived: amount_cad >= 10000",         "transformer.derive"),
    LineageEdge("TPS.transactions_raw",        "transaction_date", "staging.stg_transactions", "txn_year",          "Derived: date.year",                    "transformer.derive"),
    LineageEdge("TPS.transactions_raw",        "transaction_date", "staging.stg_transactions", "txn_quarter",       "Derived: date.quarter",                 "transformer.derive"),
    LineageEdge("staging.stg_transactions",    "transaction_id",   "marts.fact_transactions",   "transaction_id",    "Direct copy with surrogate key",        "dbt.fact_transactions"),
    LineageEdge("staging.stg_transactions",    "amount_cad",       "marts.fact_transactions",   "amount_cad",        "Direct copy",                           "dbt.fact_transactions"),
    LineageEdge("staging.stg_transactions",    "amount_cad",       "metrics.kpi_daily_summary", "total_volume_cad",  "Aggregated: SUM(amount_cad) per date",  "dbt.kpi_daily"),
    LineageEdge("staging.stg_transactions",    "transaction_id",   "metrics.kpi_daily_summary", "transaction_count", "Aggregated: COUNT(*) per date",         "dbt.kpi_daily"),

    # ── Credit lineage ────────────────────────────────────────────────────────

    LineageEdge("CREDIT_RISK.credit_data_raw", "loan_id",          "staging.stg_credit",       "loan_id",           "Direct copy",                           "extractor"),
    LineageEdge("CREDIT_RISK.credit_data_raw", "credit_score",     "staging.stg_credit",       "credit_score_band", "Derived: pd.cut on credit_score",       "transformer.derive"),
    LineageEdge("CREDIT_RISK.credit_data_raw", "days_past_due",    "staging.stg_credit",       "is_delinquent",     "Derived: days_past_due > 0",             "transformer.derive"),
    LineageEdge("CREDIT_RISK.credit_data_raw", "days_past_due",    "staging.stg_credit",       "delinquency_bucket","Derived: pd.cut on days_past_due",      "transformer.derive"),
    LineageEdge("CREDIT_RISK.credit_data_raw", "outstanding_balance","staging.stg_credit",     "ltv_ratio",         "Derived: outstanding / original_amount", "transformer.derive"),
]


# ── LineageGraph ───────────────────────────────────────────────────────────────

class LineageGraph:
    """Query upstream ancestors and downstream descendants of any field."""

    def __init__(self, edges: list[LineageEdge] = LINEAGE_EDGES):
        self.edges = edges

    def _key(self, dataset: str, column: str) -> str:
        return f"{dataset}.{column}"

    def ancestors(self, dataset: str, column: str, depth: int = 10) -> list[LineageEdge]:
        """Return all upstream edges that feed into (dataset, column)."""
        results = []
        queue = [(dataset, column)]
        seen = set()

        for _ in range(depth):
            if not queue:
                break
            next_queue = []
            for ds, col in queue:
                key = self._key(ds, col)
                if key in seen:
                    continue
                seen.add(key)
                for edge in self.edges:
                    if edge.target_dataset == ds and edge.target_column == col:
                        results.append(edge)
                        next_queue.append((edge.source_dataset, edge.source_column))
            queue = next_queue

        return results

    def descendants(self, dataset: str, column: str, depth: int = 10) -> list[LineageEdge]:
        """Return all downstream edges produced from (dataset, column)."""
        results = []
        queue = [(dataset, column)]
        seen = set()

        for _ in range(depth):
            if not queue:
                break
            next_queue = []
            for ds, col in queue:
                key = self._key(ds, col)
                if key in seen:
                    continue
                seen.add(key)
                for edge in self.edges:
                    if edge.source_dataset == ds and edge.source_column == col:
                        results.append(edge)
                        next_queue.append((edge.target_dataset, edge.target_column))
            queue = next_queue

        return results

    def print_lineage(self, dataset: str, column: str) -> None:
        """Print full ancestry + descendants for a field."""
        print(f"\n{'═'*65}")
        print(f"  LINEAGE REPORT — {dataset}.{column}")
        print(f"{'═'*65}")

        ancestors = self.ancestors(dataset, column)
        print(f"\n  ◀ UPSTREAM ANCESTORS ({len(ancestors)} hops):")
        for e in reversed(ancestors):
            print(f"    {e.source_dataset}.{e.source_column}")
            print(f"      → [{e.pipeline_step}] {e.transformation_rule}")
            print(f"      → {e.target_dataset}.{e.target_column}")

        descendants = self.descendants(dataset, column)
        print(f"\n  ▶ DOWNSTREAM DESCENDANTS ({len(descendants)} hops):")
        for e in descendants:
            print(f"    {e.source_dataset}.{e.source_column}")
            print(f"      → [{e.pipeline_step}] {e.transformation_rule}")
            print(f"      → {e.target_dataset}.{e.target_column}")

        print(f"{'═'*65}\n")

    def export_json(self, output_path: Path | None = None) -> dict:
        """Export full lineage graph to JSON."""
        payload = {
            "generated_at": __import__("datetime").datetime.utcnow().isoformat(),
            "total_edges": len(self.edges),
            "edges": [asdict(e) for e in self.edges],
        }
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(payload, f, indent=2)
            print(f"Lineage graph exported → {output_path}")
        return payload

    def export_markdown(self, output_path: Path) -> None:
        """Export lineage as Markdown table for documentation."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Data Lineage Registry\n",
            "| Source Dataset | Source Column | Target Dataset | Target Column | Transformation | Pipeline Step |",
            "|---|---|---|---|---|---|",
        ]
        for e in self.edges:
            lines.append(
                f"| {e.source_dataset} | {e.source_column} | {e.target_dataset} | "
                f"{e.target_column} | {e.transformation_rule} | {e.pipeline_step} |"
            )
        output_path.write_text("\n".join(lines))
        print(f"Lineage Markdown exported → {output_path}")


# ── CLI demo ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    graph = LineageGraph()

    # Show lineage for a KPI metric — traces all the way back to source system
    graph.print_lineage("metrics.kpi_daily_summary", "total_volume_cad")

    # Export to docs
    graph.export_json(LINEAGE_DIR / "lineage_graph.json")
    graph.export_markdown(
        Path(__file__).parents[2] / "docs" / "DATA_LINEAGE.md"
    )
