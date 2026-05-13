-- ============================================================
-- dbt Model: marts/dim_merchant.sql
-- Layer: Dimensional Mart
-- Type: Static dimension (loaded from seed file)
-- Grain: One row per merchant category
-- ============================================================

{{ config(
    materialized='table',
    tags=['marts', 'dimension', 'merchant']
) }}

WITH seed_data AS (

    SELECT * FROM {{ ref('merchant_categories') }}

),

with_sk AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY merchant_category)  AS merchant_sk,
        merchant_category,
        category_group,
        is_essential,
        CURRENT_TIMESTAMP                               AS _loaded_at
    FROM seed_data

)

SELECT * FROM with_sk
