{{ config(
    materialized='table',
    schema='silver',
    description='trans form Bronze into silver'
) }}

with state_code as (
    select * from {{ ref('state_code')}}

)
SELECT
    -- Original columns with standardized naming
    UPPER ("NAME") AS county,
    UPPER("STATE") AS state_code,
    "STATE_FIPS_CD" ,
    "LONGITUDE" AS longitude,
	"LATITUDE" AS latitude, 
    SC.state_full AS state_name,
  -- Metadata columns
    
    -- DBT metadata
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    '{{ invocation_id }}' AS dbt_batch_id,
    
FROM {{ ref('bronze_county') }} BR
JOIN state_code SC
  ON BR.STATE = SC.state_abbrev


