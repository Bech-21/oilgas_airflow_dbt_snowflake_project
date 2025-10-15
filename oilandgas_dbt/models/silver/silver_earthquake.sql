{{ config(
    materialized='table',
    schema='silver',
    description='trans form Bronze into silver'
) }}


SELECT
    -- Original columns with standardized naming
    "id",
 	"longitude",
	"latitude",
    "elevation" AS elevation,
    "magnitude",
    "magType" AS magnitude_type,
    "sig" AS significance,
    "place",
    UPPER("region") AS country,
    UPPER("county") AS county,
    UPPER("state") AS state,

    -- Event timing
    "time" AS event_time,
    YEAR("time") AS event_year,
    MONTH("time") AS event_month,
    WEEK("time") AS event_week_num,

    
    -- DBT metadata
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    '{{ invocation_id }}' AS dbt_batch_id,
    
FROM {{ source('oilgas_raw','BRONZE_EARTHQUAKE_RAW') }}  BR

WHERE "id" IS NOT NULL
  AND "time" IS NOT NULL
  AND  "magnitude" IS NOT NULL 