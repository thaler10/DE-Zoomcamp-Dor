{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
    -- Revenue grouping 
    pickup_location_id as revenue_zone, -- כאן בדרך כלל עושים Join ל-Zones, אבל נשתמש ב-ID כרגע
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month, 
    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    count(*) as total_monthly_trips -- זה מה שצריך לשאלה 3 ו-5

    from trips_data
    group by 1,2,3