-- ============================================================
-- dbt Model: marts/dim_account.sql
-- Layer: Dimensional Mart
-- Type: SCD Type 2 (history-preserving)
-- Grain: One row per account per attribute-change event
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='account_sk',
    incremental_strategy='delete+insert',
    tags=['marts', 'dimension', 'accounts']
) }}

WITH staged AS (

    SELECT * FROM {{ ref('stg_accounts') }}

),

customers AS (

    SELECT customer_id, customer_sk
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE

),

with_sk AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['a.account_id']) }}     AS account_sk,
        a.account_id,
        c.customer_sk,
        a.account_type,
        a.account_status,
        a.currency,
        a.balance_tier,
        a.branch_id,
        a.is_dormant,
        a.is_overdraft,
        a.open_date,
        CURRENT_DATE                                                  AS effective_from,
        NULL::DATE                                                    AS effective_to,
        TRUE                                                          AS is_current,
        CURRENT_TIMESTAMP                                             AS _loaded_at

    FROM staged a
    LEFT JOIN customers c ON c.customer_id = a.customer_id

)

SELECT * FROM with_sk

{% if is_incremental() %}
    WHERE account_id NOT IN (
        SELECT account_id FROM {{ this }} WHERE is_current = TRUE
    )
{% endif %}
