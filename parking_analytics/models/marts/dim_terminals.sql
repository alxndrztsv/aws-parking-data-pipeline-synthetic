{{
    config(
        materialized='table',
        tags=['dimension', 'terminals']
    )
}}

-- Terminal dimension table.

SELECT DISTINCT
    CAST(terminal_id AS VARCHAR) || '-' || CAST(park_id AS VARCHAR) AS terminal_pk,
    terminal_id,
    park_id,  -- foreign key to dim_parks
    terminal_description,
    address,
    zone_name,
    circuit_type,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS load_timestamp
FROM {{ ref('stg_parking_transactions') }}