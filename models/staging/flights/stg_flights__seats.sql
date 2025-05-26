{{
  config(
    materialized = 'table'
    )
}}
select
    aircraft_code,
    seat_no,
    fare_conditions,
    'static_value' as new_column
from {{ source('demo_src', 'seats') }}


  