{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select *
    from {{ ref('dim_fhv_trips') }}
),

cte AS (
    SELECT *,  ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY p90 DESC) AS rn
    FROM (
        SELECT DISTINCT pickup_zone, dropoff_zone, p90
        FROM (
            SELECT 
                pickup_zone, 
                dropoff_zone, 
                PERCENTILE_CONT(duration, 0.90) OVER (PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS p90
            FROM (
                SELECT 
                    pickup_zone,
                    dropoff_zone,
                    pickup_year,
                    pickup_month,
                    pickup_locationid,
                    dropoff_locationid,
                    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS duration
                FROM fhv_trips
            ) subquery
            WHERE 
                pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
                AND pickup_year = 2019
                AND pickup_month = 11
        )
    )
)

SELECT * 
FROM cte 
WHERE rn < 3
ORDER BY pickup_zone, rn



