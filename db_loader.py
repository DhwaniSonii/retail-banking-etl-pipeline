"""
Loader — PostgreSQL Target Layer

Loads staged (parquet) data into PostgreSQL star schema.
Implements upsert patterns to support incremental loads.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger("etl.loader")

PROCESSED_DIR = Path(__file__).parents[2] / "data" / "processed"

LoadMode = Literal["replace", "append", "upsert"]


def get_engine(connection_string: str | None = None) -> Engine:
    """
    Returns SQLAlchemy engine.
    Defaults to local dev DB; override via connection_string or env var.
    """
    if connection_string is None:
        import os
        connection_string = os.getenv(
            "BANKING_DB_URL",
            "postgresql+psycopg2://postgres:postgres@localhost:5432/banking_platform",
        )
    return create_engine(connection_string, pool_pre_ping=True)


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    engine: Engine,
    mode: LoadMode = "append",
    pk_columns: list[str] | None = None,
) -> int:
    """
    Load a DataFrame to PostgreSQL.
    Supports full replace, append, or upsert (delete-insert on PK).
    Returns number of rows written.
    """
    if df.empty:
        log.warning("Empty DataFrame — skipping load for %s.%s", schema, table_name)
        return 0

    target = f"{schema}.{table_name}"
    n = len(df)

    if mode == "replace":
        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False, chunksize=5_000)
        log.info("REPLACE → %s (%d rows)", target, n)

    elif mode == "append":
        df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False, chunksize=5_000)
        log.info("APPEND → %s (%d rows)", target, n)

    elif mode == "upsert":
        if not pk_columns:
            raise ValueError("pk_columns required for upsert mode")
        _upsert(df, table_name, schema, engine, pk_columns)
        log.info("UPSERT → %s (%d rows)", target, n)

    return n


def _upsert(df: pd.DataFrame, table: str, schema: str, engine: Engine, pk_columns: list[str]) -> None:
    """
    Delete-insert upsert strategy.
    For production, use PostgreSQL ON CONFLICT DO UPDATE instead.
    """
    pk_values = df[pk_columns].drop_duplicates()

    with engine.begin() as conn:
        # Build parameterized DELETE
        where_clauses = " AND ".join([f"{col} = :{col}" for col in pk_columns])
        delete_sql = text(f"DELETE FROM {schema}.{table} WHERE {where_clauses}")

        for _, row in pk_values.iterrows():
            conn.execute(delete_sql, row[pk_columns].to_dict())

    # Insert clean batch
    df.to_sql(table, engine, schema=schema, if_exists="append", index=False, chunksize=5_000)


def load_all(engine: Engine) -> None:
    """Load all staged parquet files to PostgreSQL."""
    log.info("Starting database load phase…")

    loads = [
        ("customers_staged.parquet",   "staging", "stg_customers",    "replace", ["customer_id"]),
        ("accounts_staged.parquet",    "staging", "stg_accounts",     "replace", ["account_id"]),
        ("transactions_staged.parquet","staging", "stg_transactions", "append",  ["transaction_id"]),
        ("credit_staged.parquet",      "staging", "stg_credit",       "replace", ["loan_id"]),
    ]

    total = 0
    for filename, schema, table, mode, pk_cols in loads:
        path = PROCESSED_DIR / filename
        if not path.exists():
            log.warning("Staged file not found, skipping: %s", path)
            continue
        df = pd.read_parquet(path)
        n = load_dataframe(df, table, schema, engine, mode=mode, pk_columns=pk_cols)
        total += n

    log.info("Load phase complete. Total rows written: %d", total)
