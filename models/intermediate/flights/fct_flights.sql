{{
  config(
    materialized = 'table',
    )
}}
select
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
   {{ concat_columns([ 'flight_id', 'flight_no' ]) }} as fligth_info
  from
    {{ ref('stg_flights__facts__flights') }}

 -- 3 (2/3). С помощью макроса get_column_values получить все уникальные значения статуса полетов (поле status модели fct_fligths). Вывести их в логи, при обновлении модели fct_fligths.

{% set uniqs = dbt_utils.get_column_values(table=ref('fct_flights'), column='status') %}

{% for uniq in uniqs %}
    --{{uniq}}
{% endfor %}
