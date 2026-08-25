{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

-- Staging layer. Clean and type-cast silver data.
-- Foundation for all downstream models.

WITH 
    raw_data AS (
        SELECT * FROM {{ source ('parking-pipeline-dev-parking-db', 'processed') }}
    ),
    cleaned_data AS (
        SELECT
            "payment_mean" AS payment_method,
            "origin" AS transaction_origin,
            date_parse("server_time", '%d/%m/%Y %H:%i') AS server_timestamp,
            date_parse("terminal_date", '%d/%m/%Y %H:%i') AS terminal_timestamp,
            "terminal_code" AS terminal_id,
            CAST("amount" AS DECIMAL(10, 2)) AS amount,
            CAST("total_duration_in_mins" AS INT) AS total_duration_minutes,
            CAST("paid_duration_in_mins" AS INT) AS paid_duration_minutes,
            "system_id",
            "printed_id",
            "zone_desc" AS zone_name,
            "circuit_desc" AS circuit_type,
            "park_code" AS park_id,
            "park" AS park_name,
            "terminal_description",
            "type" AS transaction_type,
            "address",
            "user_type",
            date_parse("end_date", '%d/%m/%Y %H:%i') AS end_timestamp,
            "free_duration" AS free_duration_mins,
            "currency",
            "banking_id",
            "product_name",
            "user_name"
        FROM raw_data
        WHERE 
            "system_id" IS NOT NULL AND
            "park_code" IS NOT NULL AND
            "terminal_code" IS NOT NULL
    )
SELECT * FROM cleaned_data