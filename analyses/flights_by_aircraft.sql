{% set aircraft_query %}
SELECT DISTINCT
    aircraft_code
FROM
    {{ ref('fct_flights') }}
{%endset%}        

{% set aircraft_query_result = run_query(aircraft_query) %}
{% if execute %}

{% set important_aircrafts = aircraft_query_result.columns[0].values() %}
{% else %}
{% set important_aircrafts = [] %}
{% endif %}

SELECT DISTINCT
    {%- for aircraft in important_aircrafts %}
    SUM(CASE WHEN aircraft_code = '{{ aircraft }}' THEN 1 ELSE 0 END) as flights_{{ aircraft | title | replace('73', 'oo')}}
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
FROM
    {{ ref('fct_flights') }}