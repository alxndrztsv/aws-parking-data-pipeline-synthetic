{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

-- Geenrate a continuous date spine for proper time-series analysis

WITH 
    date_range AS (
        SELECT
            MIN(CAST(terminal_timestamp AS DATE)) AS start_date,
            MAX(CAST(terminal_timestamp AS DATE)) AS end_date
        FROM {{ ref('stg_parking_transactions') }}
    ),
    date_spine AS (
        SELECT
            CAST(date_day AS DATE) AS date_day
        FROM date_range
        CROSS JOIN UNNEST(
            sequence(
                CAST(start_date AS TIMESTAMP), 
                CAST(end_date AS TIMESTAMP), 
                INTERVAL '1' DAY
            )
        ) AS t(date_day)
    )
SELECT date_day FROM date_spine