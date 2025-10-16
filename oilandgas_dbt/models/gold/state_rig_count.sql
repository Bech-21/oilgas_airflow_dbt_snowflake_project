
{{ config(
    materialized='table',
    schema='gold',
    description='State-level correlation between rig counts and earthquake activity'
) }}

WITH
    state_rankings AS (
        SELECT
            STATE_OR_PROVINCE AS state,
            COUNTY AS county,
            SUM(RIG_COUNT) AS total_rigs,
            COUNT(DISTINCT DATE) AS active_days,
            AVG(RIG_COUNT) AS avg_daily_rigs,
            MAX(RIG_COUNT) AS peak_rigs,
            RANK() OVER (
                ORDER BY
                    MAX(RIG_COUNT) DESC
            ) AS peak_rig_rank,
            RANK() OVER (
                ORDER BY
                    SUM(RIG_COUNT) DESC
            ) AS total_rig_rank,
            RANK() OVER (
                ORDER BY
                    AVG(RIG_COUNT) DESC
            ) AS avg_rig_rank
        FROM
           {{ ref('silver_rig_counts') }}
        WHERE
            STATE_OR_PROVINCE IS NOT NULL
        GROUP BY
            STATE_OR_PROVINCE,
            COUNTY
    )
SELECT
    state,
    county,
    total_rigs,
    active_days,
    avg_daily_rigs,
    peak_rigs,
    peak_rig_rank,
    total_rig_rank,
    avg_rig_rank
FROM
    state_rankings
ORDER BY
    peak_rig_rank,
    total_rig_rank