# Architectural Decision Log

Key design choices made in this project and their rationale.
This is what you'd discuss in a technical interview.

---

## 1. Kimball Star Schema over Data Vault

**Decision:** Use Kimball dimensional modeling (star schema) for the marts layer.

**Why not Data Vault?**
Data Vault excels at audit trail and handling many source systems in enterprise settings. For a retail banking analytics platform focused on serving BI/reporting consumers, Kimball's star schema is simpler to query, performs better for aggregation-heavy workloads, and is more widely understood by business analysts.

**Trade-off:** Less flexible for adding new source systems mid-flight; SCD Type 2 handles history on dimensions.

---

## 2. PostgreSQL over Snowflake/BigQuery

**Decision:** PostgreSQL as the target database.

**Why:** Portfolio project needs to be runnable locally by anyone. PostgreSQL is production-grade, open-source, and used by many financial institutions. The DDL and dbt models translate directly to Snowflake with minimal changes (swap `dbt-postgres` for `dbt-snowflake`).

**In production at a bank like TD:** Azure Synapse Analytics or Snowflake would be the target, with Azure Data Factory handling ingestion.

---

## 3. Python Custom DQ + Great Expectations (dual layer)

**Decision:** Build a lightweight custom DQ framework AND integrate Great Expectations.

**Why both?**
The custom framework (`data_quality.py`) runs inline in the pipeline with zero extra config — perfect for quarantine logic and pipeline gating. Great Expectations adds industry-standard expectation suites, shareable HTML data docs, and CI integration that a bank's data governance team would recognize.

---

## 4. SCD Type 2 for Customer and Account Dimensions

**Decision:** Implement Slowly Changing Dimension Type 2 on `dim_customer` and `dim_account`.

**Why:** Banking regulations (OSFI, FINTRAC) require point-in-time accuracy. If a customer's segment changes, we need to know what segment they were in when a transaction occurred — not just their current segment. SCD2 preserves this history via `effective_from / effective_to / is_current`.

---

## 5. Parquet as Intermediate Format

**Decision:** Write staged data to Parquet before loading to PostgreSQL.

**Why:** Parquet is columnar (fast for analytics), compressed (smaller than CSV), preserves data types, and is the industry standard for data lakes. It also decouples the transform and load stages — the pipeline can be re-run from the transform stage without re-extracting.

---

## 6. SHA-256 for PII Masking (not random masking)

**Decision:** Use deterministic SHA-256 hashing for PII fields in non-production.

**Why deterministic?** Random masking breaks joins. If `customer_id CUST001` maps to `John Smith`, a join between two tables on the masked name would fail if the mask is random. SHA-256 ensures the same input always produces the same masked output, preserving join integrity while protecting PII.

---

## 7. Airflow over Prefect/Luigi

**Decision:** Apache Airflow for orchestration.

**Why:** Airflow is the dominant orchestration tool in enterprise data teams, including financial services. TD and most major banks use it. Prefect is more modern but less prevalent in established enterprise stacks.
