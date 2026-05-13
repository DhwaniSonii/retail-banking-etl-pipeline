-- ============================================================
-- dbt Model: staging/stg_transactions.sql
-- Layer: Staging
-- Description: Cleans and types raw transaction data.
--   Removes duplicates, enforces not-null on critical fields,
--   derives date parts and signed amounts.
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    tags=['staging', 'transactions', 'daily']
) }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'transactions_raw') }}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _extract_ts DESC
        ) AS row_num
    FROM source

),

cleaned AS (

    SELECT
        transaction_id,
        account_id,
        transaction_type,
        channel,
        amount,
        currency,
        merchant_category,
        transaction_date::DATE                                          AS transaction_date,
        transaction_time::TIME                                          AS transaction_time,
        status,
        description,

        -- Currency normalization: USD → CAD
        CASE
            WHEN currency = 'USD' THEN ROUND(amount * 1.36, 2)
            ELSE amount
        END                                                             AS amount_cad,

        -- Signed amount: debits are negative cash flow
        CASE
            WHEN transaction_type IN ('Debit', 'ATM Withdrawal', 'Bill Payment')
            THEN -ROUND(amount * CASE WHEN currency = 'USD' THEN 1.36 ELSE 1 END, 2)
            ELSE  ROUND(amount * CASE WHEN currency = 'USD' THEN 1.36 ELSE 1 END, 2)
        END                                                             AS signed_amount_cad,

        -- FINTRAC large-transaction flag (≥ CAD $10,000)
        CASE
            WHEN amount * CASE WHEN currency = 'USD' THEN 1.36 ELSE 1 END >= 10000
            THEN TRUE ELSE FALSE
        END                                                             AS is_large_transaction,

        -- Date dimensions
        EXTRACT(YEAR    FROM transaction_date::DATE)::SMALLINT          AS txn_year,
        EXTRACT(MONTH   FROM transaction_date::DATE)::SMALLINT          AS txn_month,
        EXTRACT(QUARTER FROM transaction_date::DATE)::SMALLINT          AS txn_quarter,
        TO_CHAR(transaction_date::DATE, 'Day')                          AS txn_day_of_week,
        EXTRACT(ISODOW  FROM transaction_date::DATE) IN (6, 7)          AS is_weekend,

        COALESCE(merchant_category, 'Unclassified')                     AS merchant_category_clean,
        CURRENT_TIMESTAMP                                               AS _transformed_at,
        '1.0.0'                                                         AS _pipeline_version

    FROM deduplicated
    WHERE
        row_num = 1
        AND transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        AND amount IS NOT NULL

    {% if is_incremental() %}
        AND transaction_date::DATE > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}

)

SELECT * FROM cleaned
