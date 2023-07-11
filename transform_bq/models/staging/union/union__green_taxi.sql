{{ config(materialized='table', schema='stg') }}

{% set months = ['01', '02', '03'] %}
{% for month in months %}
  select 
    VendorID as vendor_id,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID as rate_code_id,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
  from {{ source('src__green_taxi', 'tripdata_2021_' + month) }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %}