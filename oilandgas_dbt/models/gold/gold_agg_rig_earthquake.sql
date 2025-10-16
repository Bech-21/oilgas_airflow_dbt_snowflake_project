{{ config(
    materialized='table',
    schema='gold',
    description='State-level correlation between rig counts and earthquake activity'
) }}



WITH rig_by_state AS (
    SELECT 
        STATE_OR_PROVINCE AS state,
        "Year" AS year,
        EVENT_WEEK_NUM,
        SUM(RIG_COUNT) AS total_rigs,
        COUNT(DISTINCT DATE) AS active_days,
        AVG(RIG_COUNT) AS avg_daily_rigs,
        MAX(RIG_COUNT) AS peak_rigs
    FROM {{ ref('silver_rig_counts') }}
    WHERE STATE_OR_PROVINCE IS NOT NULL
    GROUP BY STATE_OR_PROVINCE, "Year", EVENT_WEEK_NUM
),

earthquake_by_state AS (
    SELECT 
        STATE AS state,
        EVENT_YEAR AS year,
        EVENT_WEEK_NUM,
        COUNT(*) AS total_earthquakes,
        COUNT(DISTINCT DATE(EVENT_TIME)) AS active_days,
        AVG("magnitude") AS avg_magnitude,
        MAX("magnitude") AS max_magnitude,
        COUNT_IF("magnitude" >= 3.0) AS significant_events,
        AVG(SIGNIFICANCE) AS avg_significance
    FROM {{ ref('silver_earthquake') }}
    WHERE STATE IS NOT NULL
    GROUP BY STATE, EVENT_YEAR, EVENT_WEEK_NUM
),

combined_data AS (
    SELECT 
        r.state,
        r.year,
        r.EVENT_WEEK_NUM,
        COALESCE(r.total_rigs, 0) AS total_rigs,
        COALESCE(e.total_earthquakes, 0) AS total_earthquakes,
        COALESCE(e.avg_magnitude, 0) AS avg_magnitude
    FROM rig_by_state r
    INNER JOIN earthquake_by_state e
        ON r.state = e.state
        AND r.year = e.year
        AND r.EVENT_WEEK_NUM = e.EVENT_WEEK_NUM
)

SELECT 
    state,
    SUM(total_rigs) AS total_rigs_count,
    SUM(total_earthquakes) AS total_earthquake_count,
    AVG(avg_magnitude) AS avg_earthquake_magnitude,
    RANK() OVER (ORDER BY SUM(total_rigs) DESC) AS rig_rank,
    ROW_NUMBER() OVER (ORDER BY SUM(total_rigs) DESC) AS rig_position
FROM combined_data
GROUP BY state
ORDER BY total_rigs_count DESC

