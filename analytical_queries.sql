-- ============================================================
-- Banking Data Platform — Analytical Queries
-- These are stakeholder-facing queries that run against the
-- marts layer. Demonstrates ability to support partners
-- in interpreting and analyzing data.
-- ============================================================


-- ── 1. Daily Transaction Summary (Executive Dashboard) ──────

SELECT
    d.full_date,
    d.day_of_week,
    COUNT(*)                                            AS transaction_count,
    ROUND(SUM(f.amount_cad), 2)                         AS total_volume_cad,
    ROUND(AVG(f.amount_cad), 2)                         AS avg_amount_cad,
    COUNT(DISTINCT f.account_sk)                        AS active_accounts,
    SUM(CASE WHEN f.is_large_transaction THEN 1 ELSE 0 END) AS fintrac_reportable_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.status = 'Failed' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    )                                                   AS failure_rate_pct
FROM marts.fact_transactions f
JOIN marts.dim_date d ON d.date_sk = f.date_sk
GROUP BY d.full_date, d.day_of_week
ORDER BY d.full_date DESC;


-- ── 2. Customer Segment Analysis ────────────────────────────

SELECT
    c.customer_segment,
    c.age_band,
    c.province,
    COUNT(DISTINCT f.customer_sk)                       AS customer_count,
    COUNT(*)                                            AS total_transactions,
    ROUND(SUM(f.amount_cad), 2)                         AS total_spend_cad,
    ROUND(AVG(f.amount_cad), 2)                         AS avg_transaction_cad,
    ROUND(SUM(f.amount_cad) / NULLIF(COUNT(DISTINCT f.customer_sk), 0), 2) AS avg_spend_per_customer
FROM marts.fact_transactions f
JOIN marts.dim_customer c ON c.customer_sk = f.customer_sk AND c.is_current = TRUE
GROUP BY c.customer_segment, c.age_band, c.province
ORDER BY total_spend_cad DESC;


-- ── 3. Account Dormancy Report ───────────────────────────────

SELECT
    a.account_type,
    a.balance_tier,
    COUNT(*)                                            AS total_accounts,
    SUM(CASE WHEN a.is_dormant THEN 1 ELSE 0 END)       AS dormant_accounts,
    ROUND(
        100.0 * SUM(CASE WHEN a.is_dormant THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    )                                                   AS dormancy_rate_pct,
    ROUND(AVG(a.days_since_activity), 0)                AS avg_days_inactive
FROM staging.stg_accounts a
WHERE a.account_status = 'Active'
GROUP BY a.account_type, a.balance_tier
ORDER BY dormancy_rate_pct DESC;


-- ── 4. Channel Performance (MoM) ────────────────────────────

SELECT
    d.year,
    d.month_number,
    d.month_name,
    f.channel,
    COUNT(*)                                            AS transaction_count,
    ROUND(SUM(f.amount_cad), 2)                         AS volume_cad,
    ROUND(AVG(f.amount_cad), 2)                         AS avg_amount,

    -- Month-over-month growth
    ROUND(
        100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (PARTITION BY f.channel ORDER BY d.year, d.month_number))
        / NULLIF(LAG(COUNT(*)) OVER (PARTITION BY f.channel ORDER BY d.year, d.month_number), 0),
    2)                                                  AS txn_count_mom_pct

FROM marts.fact_transactions f
JOIN marts.dim_date d ON d.date_sk = f.date_sk
GROUP BY d.year, d.month_number, d.month_name, f.channel
ORDER BY d.year, d.month_number, volume_cad DESC;


-- ── 5. FINTRAC Large Transaction Register ───────────────────
-- Regulatory query: all transactions ≥ CAD $10,000

SELECT
    f.transaction_id,
    f.transaction_date,
    f.transaction_type,
    f.channel,
    f.amount_cad,
    a.account_id,
    a.account_type,
    a.currency,
    c.customer_id,
    c.customer_segment,
    c.kyc_status,
    c.province
FROM marts.fact_transactions f
JOIN marts.dim_account  a ON a.account_sk  = f.account_sk  AND a.is_current = TRUE
JOIN marts.dim_customer c ON c.customer_sk = f.customer_sk AND c.is_current = TRUE
WHERE f.is_large_transaction = TRUE
  AND f.status = 'Completed'
ORDER BY f.amount_cad DESC;


-- ── 6. Credit Risk Portfolio Summary ────────────────────────

SELECT
    risk_rating,
    loan_type,
    credit_score_band,
    delinquency_bucket,
    COUNT(*)                                            AS loan_count,
    ROUND(SUM(original_amount), 2)                      AS total_original_cad,
    ROUND(SUM(outstanding_balance), 2)                  AS total_outstanding_cad,
    ROUND(AVG(ltv_ratio), 4)                            AS avg_ltv,
    ROUND(AVG(credit_score), 0)                         AS avg_credit_score,
    SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END)      AS delinquent_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    )                                                   AS delinquency_rate_pct
FROM staging.stg_credit
GROUP BY risk_rating, loan_type, credit_score_band, delinquency_bucket
ORDER BY risk_rating, delinquency_rate_pct DESC;


-- ── 7. Customer Lifetime Value Proxy ────────────────────────

WITH customer_activity AS (
    SELECT
        f.customer_sk,
        COUNT(*)                                        AS lifetime_txn_count,
        ROUND(SUM(f.amount_cad), 2)                     AS lifetime_volume_cad,
        MIN(f.transaction_date)                         AS first_txn_date,
        MAX(f.transaction_date)                         AS last_txn_date,
        COUNT(DISTINCT f.account_sk)                    AS account_count
    FROM marts.fact_transactions f
    WHERE f.status = 'Completed'
    GROUP BY f.customer_sk
)

SELECT
    c.customer_segment,
    c.age_band,
    c.province,
    COUNT(*)                                            AS customer_count,
    ROUND(AVG(a.lifetime_volume_cad), 2)                AS avg_lifetime_value_cad,
    ROUND(AVG(a.lifetime_txn_count), 0)                 AS avg_txn_count,
    ROUND(AVG(a.account_count), 1)                      AS avg_products_held,
    ROUND(AVG(
        DATE_PART('day', a.last_txn_date::TIMESTAMP - a.first_txn_date::TIMESTAMP) / 365.25
    ), 1)                                               AS avg_active_years
FROM customer_activity a
JOIN marts.dim_customer c ON c.customer_sk = a.customer_sk AND c.is_current = TRUE
GROUP BY c.customer_segment, c.age_band, c.province
ORDER BY avg_lifetime_value_cad DESC;
