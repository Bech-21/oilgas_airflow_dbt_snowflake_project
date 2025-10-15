

{{ config(
    materialized='table',
    schema='silver',
    description='trans form Bronze into silver'
) }}
SELECT
    -- Original columns with standardized naming
    UPPER ("Country") AS country,
    UPPER("County") AS county,
    UPPER("Basin") AS basin,-- Standardize to uppercase
    "DrillFor" AS drill_for,
    "State/Province" AS state_or_province,
    UPPER("Location") AS location,
    "Trajectory" AS trajectory,
    DATE("US_PublishDate") AS date, 
    "Year",
    "Month", 
    WEEK(DATE("US_PublishDate")) AS event_week_num,
    "Rig Count Value" AS rig_count,   
    -- Metadata columns
    
    -- DBT metadata
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    '{{ invocation_id }}' AS dbt_batch_id,
    
FROM {{ ref('bronze_rig_count') }}
WHERE UPPER("Country") = 'UNITED STATES'-- Ensure country is present }