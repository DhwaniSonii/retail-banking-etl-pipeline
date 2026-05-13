-- ============================================================
-- dbt Model: marts/dim_date.sql
-- Layer: Dimensional Mart
-- Type: Static dimension (loaded from seed)
-- Grain: One row per calendar date
-- ============================================================

{{ config(
    materialized='table',
    tags=['marts', 'dimension', 'date']
) }}

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

),

enriched AS (

    SELECT
        -- Surrogate key: YYYYMMDD integer
        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER)          AS date_sk,
        date_day                                                  AS full_date,

        -- Day attributes
        TO_CHAR(date_day, 'Day')                                  AS day_of_week,
        EXTRACT(DOW FROM date_day)::SMALLINT                      AS day_of_week_num,   -- 0=Sun, 6=Sat
        EXTRACT(DAY FROM date_day)::SMALLINT                      AS day_of_month,
        EXTRACT(DOY FROM date_day)::SMALLINT                      AS day_of_year,

        -- Week attributes
        EXTRACT(WEEK FROM date_day)::SMALLINT                     AS week_of_year,
        EXTRACT(ISODOW FROM date_day) IN (6, 7)                   AS is_weekend,

        -- Month attributes
        EXTRACT(MONTH FROM date_day)::SMALLINT                    AS month_number,
        TO_CHAR(date_day, 'Month')                                AS month_name,
        TO_CHAR(date_day, 'Mon')                                  AS month_short,
        DATE_TRUNC('month', date_day)::DATE                       AS first_day_of_month,
        (DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,

        -- Quarter attributes
        EXTRACT(QUARTER FROM date_day)::SMALLINT                  AS quarter,
        'Q' || EXTRACT(QUARTER FROM date_day)::TEXT               AS quarter_name,
        DATE_TRUNC('quarter', date_day)::DATE                     AS first_day_of_quarter,

        -- Year attributes
        EXTRACT(YEAR FROM date_day)::SMALLINT                     AS year,
        EXTRACT(YEAR FROM date_day)::TEXT || '-Q' ||
            EXTRACT(QUARTER FROM date_day)::TEXT                  AS fiscal_period,

        -- Canadian statutory holidays (federal)
        CASE
            WHEN TO_CHAR(date_day, 'MM-DD') = '01-01' THEN TRUE   -- New Year's Day
            WHEN TO_CHAR(date_day, 'MM-DD') = '07-01' THEN TRUE   -- Canada Day
            WHEN TO_CHAR(date_day, 'MM-DD') = '11-11' THEN TRUE   -- Remembrance Day
            WHEN TO_CHAR(date_day, 'MM-DD') = '12-25' THEN TRUE   -- Christmas Day
            WHEN TO_CHAR(date_day, 'MM-DD') = '12-26' THEN TRUE   -- Boxing Day
            -- Good Friday & Victoria Day require dynamic calculation (simplified here)
            ELSE FALSE
        END                                                        AS is_holiday_ca

    FROM date_spine

)

SELECT * FROM enriched
ORDER BY full_date
