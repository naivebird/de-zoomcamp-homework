{{
    config(
        materialized='table'
    )
}}

with trip_data as (
    select *
    from {{ ref('fact_trips') }}
    where 
        fare_amount > 0 and
        trip_distance > 0 and
        payment_type_description in ('Cash', 'Credit card')
        )

SELECT DISTINCT service_type, pickup_year, pickup_month, p97, p95, p90
FROM (
    SELECT 
        service_type, 
        pickup_year, 
        pickup_month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS p90
    FROM trip_data    
) subquery 
WHERE 
pickup_year = 2020 
AND pickup_month = 4
