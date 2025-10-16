{{ config(
    materialized='table',
    schema='gold',
    description='State-level correlation between rig counts and earthquake activity'
) }}




WITH earthquake_analysis AS (
    SELECT 
        state,
        county,
        COUNT(*) AS total_earthquakes,
        COUNT(DISTINCT event_time) AS active_days,
        AVG("magnitude" ) AS avg_magnitude,
        MAX("magnitude" ) AS max_magnitude,
        SUM(CASE WHEN "magnitude"  >= 4.0 THEN 1 ELSE 0 END) AS significant_quakes
    
    FROM {{ ref('silver_earthquake') }}
    WHERE state IS NOT NULL AND county IS NOT NULL
    GROUP BY state, county
)
    SELECT 
        state,
        county,
        total_earthquakes,
        active_days,
        avg_magnitude,
        max_magnitude,
        significant_quakes,

        -- Ranking by different metrics
        RANK() OVER (ORDER BY total_earthquakes DESC) AS rank_by_frequency,
        RANK() OVER (ORDER BY max_magnitude DESC) AS rank_by_max_magnitude,
        RANK() OVER (ORDER BY significant_quakes DESC) AS rank_by_significant_quakes

    FROM earthquake_analysis

