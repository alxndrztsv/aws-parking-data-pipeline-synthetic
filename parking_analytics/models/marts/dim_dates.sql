{{
    config(
        materialized='table',
        tags=['dimension', 'dates']
    )
}}

-- Date dimension table.

SELECT
    date_day AS date_id,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    DAY_OF_WEEK(date_day) AS day_of_week,
    QUARTER(date_day) AS quarter,
    CASE
        WHEN DAY_OF_WEEK(date_day) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM {{ ref('int_date_spine') }}