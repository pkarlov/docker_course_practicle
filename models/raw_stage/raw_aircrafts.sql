{{
  config(
    materialized = 'table',
    )
}}
select
    aircraft_code,
    model,
    "range",
    'bookings' as RECORD_SOURCE,
    now() as LOAD_DATE    
from
    {{ source('demo_src', 'aircrafts') }}