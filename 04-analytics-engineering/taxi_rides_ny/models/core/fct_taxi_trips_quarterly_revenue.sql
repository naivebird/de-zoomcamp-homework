{{
    config(
        materialized='table'
    )
}}

with fact_trips as (
    select *
    from {{ ref('fact_trips') }}
),

quarterly_revenue_2019 as (
    SELECT pickup_quarter, service_type, SUM(total_amount) AS total_amount_2019
    FROM fact_trips
    WHERE pickup_year = 2019
    GROUP BY pickup_quarter, service_type
    ORDER BY pickup_quarter, service_type
),

quarterly_revenue_2020 as (
    SELECT pickup_quarter, service_type, SUM(total_amount) AS total_amount_2020
    FROM fact_trips
    WHERE pickup_year = 2020
    GROUP BY pickup_quarter, service_type
    ORDER BY pickup_quarter, service_type
)

SELECT 
    qr19.pickup_quarter, 
    qr19.service_type, 
    qr19.total_amount_2019,
    qr20.total_amount_2020, 
    (qr20.total_amount_2020 - qr19.total_amount_2019) / qr19.total_amount_2019 * 100 as yoy
FROM quarterly_revenue_2019 qr19
JOIN quarterly_revenue_2020 qr20
ON qr19.pickup_quarter = qr20.pickup_quarter
AND qr19.service_type = qr20.service_type
ORDER BY qr19.service_type, yoy 

