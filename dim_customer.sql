-- ============================================================
-- dbt Model: marts/dim_customer.sql
-- Layer: Dimensional Mart
-- Type: SCD Type 2 (history-preserving)
-- Grain: One row per customer per attribute-change event
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='customer_sk',
    incremental_strategy='delete+insert',
    tags=['marts', 'dimension', 'customers']
) }}

WITH staged AS (

    SELECT * FROM {{ ref('stg_customers') }}

),

-- Assign surrogate key using MD5 hash of natural key + effective date
-- In production, use dbt_utils.generate_surrogate_key()
with_sk AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}     AS customer_sk,
        customer_id,
        customer_segment,
        kyc_status,
        province_code                                               AS province,
        age_band,
        tenure_years,
        is_active,
        CURRENT_DATE                                                AS effective_from,
        NULL::DATE                                                  AS effective_to,
        TRUE                                                        AS is_current,
        CURRENT_TIMESTAMP                                           AS _loaded_at

    FROM staged

)

SELECT * FROM with_sk

{% if is_incremental() %}
    WHERE customer_id NOT IN (
        SELECT customer_id FROM {{ this }} WHERE is_current = TRUE
    )
{% endif %}
