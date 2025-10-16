{{ config(
    materialized='view',
    schema='bronze',
    description='Raw rig count of earthquake data with minimal transformations and added metadata.'
) }}

SELECT *
FROM {{ source('oilgas_raw','BRONZE_EARTHQUAKE_RAW') }}
