-- ============================================================
-- dbt Model: staging/stg_customers.sql
-- Layer: Staging
-- Description: Cleans and conforms raw customer data from
--   Core Banking System. Applies PII masking, derives age
--   bands and tenure, enforces data types.
-- ============================================================

{{ config(
    materialized='table',
    tags=['staging', 'customers', 'daily']
) }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'customers_raw') }}

),

cleaned AS (

    SELECT
        customer_id,

        -- PII: masked in non-prod via dbt env var
        CASE
            WHEN '{{ env_var("DBT_ENV", "dev") }}' != 'prod'
            THEN 'MASKED_' || UPPER(SUBSTR(MD5(first_name || last_name), 1, 12))
            ELSE first_name
        END                                                         AS first_name,

        CASE
            WHEN '{{ env_var("DBT_ENV", "dev") }}' != 'prod'
            THEN 'MASKED_' || UPPER(SUBSTR(MD5(last_name || customer_id), 1, 12))
            ELSE last_name
        END                                                         AS last_name,

        CASE
            WHEN '{{ env_var("DBT_ENV", "dev") }}' != 'prod'
            THEN 'MASKED_' || UPPER(SUBSTR(MD5(email), 1, 12)) || '@masked.internal'
            ELSE email
        END                                                         AS email,

        province,
        UPPER(TRIM(province))                                       AS province_code,
        customer_segment,
        COALESCE(kyc_status, 'Unknown')                             AS kyc_status,
        join_date::DATE                                             AS join_date,
        date_of_birth::DATE                                         AS date_of_birth,
        is_active,

        -- Derived: age in years
        DATE_PART('year', AGE(CURRENT_DATE, date_of_birth::DATE))::SMALLINT AS age_years,

        -- Derived: tenure in years (rounded to 1 decimal)
        ROUND(
            DATE_PART('day', NOW() - join_date::TIMESTAMP) / 365.25, 1
        )                                                           AS tenure_years,

        -- Derived: age band for segmentation
        CASE
            WHEN DATE_PART('year', AGE(CURRENT_DATE, date_of_birth::DATE)) < 26  THEN '18-25'
            WHEN DATE_PART('year', AGE(CURRENT_DATE, date_of_birth::DATE)) < 36  THEN '26-35'
            WHEN DATE_PART('year', AGE(CURRENT_DATE, date_of_birth::DATE)) < 51  THEN '36-50'
            WHEN DATE_PART('year', AGE(CURRENT_DATE, date_of_birth::DATE)) < 66  THEN '51-65'
            ELSE '65+'
        END                                                         AS age_band,

        CURRENT_TIMESTAMP                                           AS _transformed_at,
        '1.0.0'                                                     AS _pipeline_version

    FROM source
    WHERE customer_id IS NOT NULL

)

SELECT * FROM cleaned
