-- ============================================================
-- dbt Model: metrics/kpi_daily_summary.sql
-- Layer: Metrics
-- Description: Daily transaction KPIs for executive dashboards.
--   Tracks volume, value, failures, and FINTRAC-reportable activity.
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='summary_date',
    tags=['metrics', 'kpi', 'daily']
) }}

WITH fact AS (

    SELECT * FROM {{ ref('fact_transactions') }}

    {% if is_incremental() %}
        WHERE transaction_date > (SELECT MAX(summary_date) FROM {{ this }})
    {% endif %}

),

daily AS (

    SELECT
        transaction_date                                        AS summary_date,

        COUNT(*)                                                AS transaction_count,
        SUM(amount_cad)                                         AS total_volume_cad,
        AVG(amount_cad)                                         AS avg_transaction_cad,
        COUNT(DISTINCT account_sk)                              AS unique_accounts,
        COUNT(DISTINCT customer_sk)                             AS unique_customers,

        -- Failure rate (Completed + Pending are non-failures)
        ROUND(
            100.0 * SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
        2)                                                      AS failure_rate_pct,

        -- FINTRAC large transaction count
        SUM(CASE WHEN is_large_transaction THEN 1 ELSE 0 END)  AS large_txn_count,

        -- Weekend vs weekday split
        SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END)            AS weekend_txn_count,
        SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END)        AS weekday_txn_count,

        -- Channel breakdown
        SUM(CASE WHEN channel = 'Online'  THEN 1 ELSE 0 END)   AS online_count,
        SUM(CASE WHEN channel = 'Mobile'  THEN 1 ELSE 0 END)   AS mobile_count,
        SUM(CASE WHEN channel = 'ATM'     THEN 1 ELSE 0 END)   AS atm_count,
        SUM(CASE WHEN channel = 'Branch'  THEN 1 ELSE 0 END)   AS branch_count,

        CURRENT_TIMESTAMP                                       AS _computed_at

    FROM fact
    GROUP BY transaction_date

)

SELECT * FROM daily


-- ============================================================
-- dbt Model: metrics/kpi_customer_segment.sql
-- Layer: Metrics
-- Description: Monthly KPIs broken down by customer segment.
--   Used by Retail Banking leadership reporting.
-- ============================================================
