-- ============================================================
-- dbt Model: marts/fact_transactions.sql
-- Layer: Dimensional Mart
-- Grain: One row per financial transaction
-- Description: Central fact table joining transactions to
--   all dimensions. All monetary values in CAD.
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    partition_by={
        'field': 'transaction_date',
        'data_type': 'date',
        'granularity': 'month'
    },
    cluster_by=['account_sk', 'customer_sk'],
    tags=['marts', 'fact', 'transactions', 'daily']
) }}

WITH transactions AS (

    SELECT * FROM {{ ref('stg_transactions') }}

),

accounts AS (

    SELECT
        account_id,
        account_sk,
        customer_sk
    FROM {{ ref('dim_account') }}
    WHERE is_current = TRUE

),

merchants AS (

    SELECT
        merchant_category,
        merchant_sk
    FROM {{ ref('dim_merchant') }}

),

dim_date AS (

    SELECT date_sk, full_date
    FROM {{ ref('dim_date') }}

),

joined AS (

    SELECT
        t.transaction_id,
        a.account_sk,
        a.customer_sk,
        d.date_sk,
        m.merchant_sk,

        t.transaction_type,
        t.channel,
        t.status,
        t.amount_cad,
        t.signed_amount_cad,
        t.is_large_transaction,
        t.is_weekend,
        t.transaction_date,

        CURRENT_TIMESTAMP                                   AS _loaded_at

    FROM transactions t

    -- Inner join: transactions with no valid account are excluded (quarantined upstream)
    INNER JOIN accounts a
        ON t.account_id = a.account_id

    -- Date dimension join
    LEFT JOIN dim_date d
        ON d.full_date = t.transaction_date

    -- Merchant dimension join (nullable — 'Unclassified' category always exists)
    LEFT JOIN merchants m
        ON m.merchant_category = t.merchant_category_clean

    WHERE t.transaction_id IS NOT NULL
        AND t.amount_cad IS NOT NULL

    {% if is_incremental() %}
        AND t.transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}

)

SELECT * FROM joined
