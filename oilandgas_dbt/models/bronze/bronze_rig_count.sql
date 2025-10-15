{{ config(
    materialized='view',
    schema='bronze',
    description='Raw rig count data from Baker Hughes with minimal transformations and added metadata.'
) }}

SELECT*
FROM {{ source('oilgas_raw','BRONZE_RIG_COUNT_RAW') }}
WHERE "Country" IS NOT NULL
  AND "Rig Count Value" IS NOT NULL