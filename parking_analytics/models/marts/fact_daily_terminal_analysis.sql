{{
    config(
        materialized='table',
        tags=['fact', 'revenue']
    )
}}

-- Daily terminal analysis fact table.

WITH daily_metrics AS (
    SELECT
        DATE(terminal_timestamp) AS transaction_date,
        terminal_id,
        park_id,
        park_name,
        zone_name,
        circuit_type,
        address,
        SUM(amount) AS total_revenue,
        COUNT(system_id) AS total_transactions,
        ROUND(SUM(amount) * 1.0 / COUNT(system_id), 2) AS avg_transaction_value,
        AVG(total_duration_minutes) AS avg_duration_minutes,
        AVG(paid_duration_minutes) AS avg_paid_duration_minutes,
        COUNT(CASE WHEN payment_method = 'Coins' THEN 1 END) AS coins_transactions,
        COUNT(CASE WHEN payment_method = 'CARD' THEN 1 END) AS card_transactions
        
    FROM {{ ref('stg_parking_transactions') }}
    WHERE terminal_timestamp IS NOT NULL
    GROUP BY 
        DATE(terminal_timestamp),
        terminal_id,
        park_id,
        park_name,
        zone_name,
        circuit_type,
        address
)

SELECT
    transaction_date,
    terminal_id,
    park_id,
    park_name,
    zone_name,
    circuit_type,
    address,
    total_revenue,
    total_transactions,
    avg_transaction_value,
    avg_duration_minutes,
    avg_paid_duration_minutes,
    coins_transactions,
    card_transactions,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS loaded_at
FROM daily_metrics