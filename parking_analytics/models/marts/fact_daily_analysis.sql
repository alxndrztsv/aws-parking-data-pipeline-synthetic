{{
    config(
        materialized='table',
        tags=['fact', 'payments']
    )
}}

-- Daily analysis fact table.

SELECT
    DATE(terminal_timestamp) AS transaction_date,
    payment_method,
    COUNT(system_id) AS transaction_count,
    SUM(amount) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    AVG(total_duration_minutes) AS avg_duration_minutes
FROM {{ ref('stg_parking_transactions') }}
WHERE payment_method IS NOT NULL
GROUP BY 
    DATE(terminal_timestamp),
    payment_method