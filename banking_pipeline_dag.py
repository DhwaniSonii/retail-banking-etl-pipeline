"""
Airflow DAG — Banking Retail Data Pipeline

Orchestrates the full ETL pipeline on a daily schedule:
  extract → validate → transform → load → dbt_run → kpi_compute → lineage_export

DAG design follows TD data platform patterns:
  - Retry logic with exponential backoff
  - SLA alerts for each critical task
  - Data quality gate: pipeline halts if score < 70
  - All tasks emit structured logs for audit trail
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sys

# Make project importable from Airflow worker
sys.path.insert(0, str(Path(__file__).parents[2]))

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ───────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-platform-team",
    "depends_on_past": False,
    "email": ["data-alerts@bank.internal"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# ── Python callables ───────────────────────────────────────────────────────────

def task_extract(**context) -> dict:
    """Extract raw data from all upstream source systems."""
    from etl.extractors.generate_banking_data import run_extraction
    datasets = run_extraction()
    context["ti"].xcom_push(key="row_counts", value={k: len(v) for k, v in datasets.items()})
    return {"status": "ok", "datasets": list(datasets.keys())}


def task_validate_transactions(**context) -> dict:
    """Run data quality checks on raw transaction data."""
    import pandas as pd
    from pathlib import Path
    from etl.transformers.data_quality import TransactionValidator, generate_report

    raw_path = Path("data/raw/transactions_raw.csv")
    accounts_path = Path("data/raw/accounts_raw.csv")

    txn_df = pd.read_csv(raw_path)
    acct_ids = set(pd.read_csv(accounts_path)["account_id"])

    validator = TransactionValidator(acct_ids)
    clean_df, checks = validator.validate(txn_df)
    quarantined = len(txn_df) - len(clean_df)

    report = generate_report("transactions_raw", clean_df, checks, quarantined)
    context["ti"].xcom_push(key="dq_score_transactions", value=report.overall_score)
    context["ti"].xcom_push(key="pipeline_action", value=report.pipeline_action)

    return {"score": report.overall_score, "action": report.pipeline_action}


def task_validate_accounts(**context) -> dict:
    """Run data quality checks on raw account data."""
    import pandas as pd
    from pathlib import Path
    from etl.transformers.data_quality import AccountValidator, generate_report

    acct_df = pd.read_csv(Path("data/raw/accounts_raw.csv"))
    cust_ids = set(pd.read_csv(Path("data/raw/customers_raw.csv"))["customer_id"])

    validator = AccountValidator(cust_ids)
    clean_df, checks = validator.validate(acct_df)

    report = generate_report("accounts_raw", clean_df, checks, 0)
    context["ti"].xcom_push(key="dq_score_accounts", value=report.overall_score)
    return {"score": report.overall_score, "action": report.pipeline_action}


def task_dq_gate(**context) -> str:
    """
    Branch: check if quality scores are sufficient to proceed.
    Returns task_id to route to (proceed or halt).
    """
    ti = context["ti"]
    txn_score = ti.xcom_pull(task_ids="validate_transactions", key="dq_score_transactions") or 0
    acct_score = ti.xcom_pull(task_ids="validate_accounts", key="dq_score_accounts") or 0

    min_score = min(txn_score, acct_score)
    if min_score < 70:
        return "halt_pipeline"
    return "transform_customers"


def task_transform_customers(**context) -> None:
    import pandas as pd
    from etl.transformers.transform import transform_customers
    df = pd.read_csv("data/raw/customers_raw.csv")
    transform_customers(df)


def task_transform_accounts(**context) -> None:
    import pandas as pd
    from etl.transformers.transform import transform_accounts
    df = pd.read_csv("data/raw/accounts_raw.csv")
    transform_accounts(df)


def task_transform_transactions(**context) -> None:
    import pandas as pd
    from etl.transformers.transform import transform_transactions
    df = pd.read_csv("data/raw/transactions_raw.csv")
    transform_transactions(df)


def task_transform_credit(**context) -> None:
    import pandas as pd
    from etl.transformers.transform import transform_credit
    df = pd.read_csv("data/raw/credit_data_raw.csv")
    transform_credit(df)


def task_load_to_db(**context) -> None:
    from etl.loaders.db_loader import get_engine, load_all
    engine = get_engine()
    load_all(engine)


def task_export_lineage(**context) -> None:
    from pathlib import Path
    from governance.lineage.lineage_graph import LineageGraph
    graph = LineageGraph()
    graph.export_json(Path("governance/lineage/lineage_graph.json"))
    graph.export_markdown(Path("docs/DATA_LINEAGE.md"))


def task_compute_kpis(**context) -> None:
    """Compute and persist daily KPI summary from staged transactions."""
    import pandas as pd
    from pathlib import Path

    txn = pd.read_parquet(Path("data/processed/transactions_staged.parquet"))

    daily = (
        txn.groupby("transaction_date")
        .agg(
            transaction_count=("transaction_id", "count"),
            total_volume_cad=("amount_cad", "sum"),
            avg_transaction_cad=("amount_cad", "mean"),
            unique_accounts=("account_id", "nunique"),
            large_txn_count=("is_large_transaction", "sum"),
        )
        .round(2)
        .reset_index()
    )

    out = Path("data/processed/kpi_daily_summary.parquet")
    daily.to_parquet(out, index=False)
    print(f"KPI summary written → {out} ({len(daily)} days)")


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="banking_retail_pipeline",
    description="Daily retail banking ETL: extract → validate → transform → load → dbt → KPIs",
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *",   # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "retail", "etl", "data-platform"],
    doc_md="""
## Banking Retail Data Pipeline

Runs nightly at 02:00 EST. Ingests data from three upstream systems:
- **Core Banking System** (customers, accounts)
- **Transaction Processing System** (debit/credit transactions)
- **Credit Risk System** (loans, credit scores)

Data quality checks gate the pipeline — score < 70 triggers a halt and alert.

**SLA:** All tasks must complete within 2 hours of scheduled start.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_upstream_data",
        python_callable=task_extract,
        sla=timedelta(minutes=20),
    )

    validate_txn = PythonOperator(
        task_id="validate_transactions",
        python_callable=task_validate_transactions,
        sla=timedelta(minutes=15),
    )

    validate_acct = PythonOperator(
        task_id="validate_accounts",
        python_callable=task_validate_accounts,
        sla=timedelta(minutes=10),
    )

    dq_gate = BranchPythonOperator(
        task_id="dq_quality_gate",
        python_callable=task_dq_gate,
    )

    halt = EmptyOperator(
        task_id="halt_pipeline",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    transform_cust = PythonOperator(task_id="transform_customers", python_callable=task_transform_customers)
    transform_acct = PythonOperator(task_id="transform_accounts",  python_callable=task_transform_accounts)
    transform_txn  = PythonOperator(task_id="transform_transactions", python_callable=task_transform_transactions)
    transform_cred = PythonOperator(task_id="transform_credit",    python_callable=task_transform_credit)

    load_db = PythonOperator(
        task_id="load_to_database",
        python_callable=task_load_to_db,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        sla=timedelta(minutes=30),
    )

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command="cd {{ var.value.get('dbt_project_dir', 'dbt_project') }} && dbt run --profiles-dir . && dbt test",
        sla=timedelta(minutes=20),
    )

    compute_kpis = PythonOperator(
        task_id="compute_kpis",
        python_callable=task_compute_kpis,
    )

    export_lineage = PythonOperator(
        task_id="export_lineage",
        python_callable=task_export_lineage,
    )

    end = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    (
        start
        >> extract
        >> [validate_txn, validate_acct]
        >> dq_gate
        >> [halt, transform_cust]
    )

    (
        transform_cust
        >> [transform_acct, transform_txn, transform_cred]
        >> load_db
        >> dbt_run
        >> [compute_kpis, export_lineage]
        >> end
    )
