{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['flight_id'],
    on_schema_change = 'sync_all_columns'
    )
}}
select
    flight_id,
    flight_no::varchar(10) as flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    'Hi everyone' new_column

from {{ source('demo_src', 'flights') }}
{#{% if is_incremental() %}
where 
    scheduled_departure > (SELECT MAX(scheduled_departure) FROM {{ source('demo_src', 'flights') }}) - interval '100 day'
{% endif %}#}
{#{% set fligths_relation = load_relation(ref('stg_flights__facts__flights')) %}
{%- set columns = adapter.get_columns_in_relation(fligths_relation) -%}

{% for column in columns -%}
  {{ "Column: " ~ column }}
{% endfor %}#}
  