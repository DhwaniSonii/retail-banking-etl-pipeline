-- ============================================================
-- dbt Model: staging/stg_accounts.sql
-- Layer: Staging
-- Description: Cleans account data from Core Banking System.
--   Derives dormancy flags, balance tiers, account age,
--   and overdraft indicators.
-- ============================================================

{{ config(
    materialized='table',
    tags=['staging', 'accounts', 'daily']
) }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'accounts_raw') }}

),

cleaned AS (

    SELECT
        account_id,
        customer_id,
        account_type,
        account_status,
        currency,
        current_balance,
        COALESCE(available_balance, current_balance)        AS available_balance,
        interest_rate,
        open_date::DATE                                     AS open_date,
        last_activity_date::DATE                            AS last_activity_date,
        branch_id,

        -- Derived: account age in days
        (CURRENT_DATE - open_date::DATE)                    AS account_age_days,

        -- Derived: inactivity in days
        (CURRENT_DATE - last_activity_date::DATE)           AS days_since_activity,

        -- Derived: dormancy flag — TD policy: 730 days with no activity on Active account
        CASE
            WHEN account_status = 'Active'
             AND (CURRENT_DATE - last_activity_date::DATE) > 730
            THEN TRUE
            ELSE FALSE
        END                                                 AS is_dormant,

        -- Derived: overdraft indicator
        current_balance < 0                                 AS is_overdraft,

        -- Derived: balance tier
        CASE
            WHEN current_balance < 0             THEN 'Negative'
            WHEN current_balance < 1000          THEN 'Under $1K'
            WHEN current_balance < 10000         THEN '$1K-$10K'
            WHEN current_balance < 50000         THEN '$10K-$50K'
            WHEN current_balance < 100000        THEN '$50K-$100K'
            ELSE                                      'Over $100K'
        END                                                 AS balance_tier,

        CURRENT_TIMESTAMP                                   AS _transformed_at,
        '1.0.0'                                             AS _pipeline_version

    FROM source
    WHERE
        account_id IS NOT NULL
        AND customer_id IS NOT NULL

)

SELECT * FROM cleaned
