{{ config(
    materialized='view',
    schema='bronze',
    description='Raw county data'
) }}

SELECT *
FROM {{ source('oilgas_raw','US_COUNTIES_CENTROIDS') }}
