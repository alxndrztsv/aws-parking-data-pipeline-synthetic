{{
    config(
        materialized='table',
        tags=['dimension', 'parks']
    )
}}

-- Park dimension table.

SELECT DISTINCT
    park_id,
    park_name,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS load_timestamp
FROM {{ ref('stg_parking_transactions') }}