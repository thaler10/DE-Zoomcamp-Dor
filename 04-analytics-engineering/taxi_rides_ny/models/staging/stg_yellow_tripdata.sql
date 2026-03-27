{{ config(materialized='view') }}
 
select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(fare_amount as numeric) as fare_amount,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type
from {{ source('staging','yellow_tripdata') }}
where vendorid is not null