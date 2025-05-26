{% set current_date = run_started_at | string | truncate(10, True, "")   %}
{% set current_year = run_started_at | string | truncate(4, True, "") | int  %}
{% set prev_year = current_year - 10 %}

SELECT 
    COUNT(*) as {{adapter.quote('select some data ')}}
FROM
    {{ ref('fct_flights') }}
WHERE 
    scheduled_departure BETWEEN '{{ current_date }}' AND '{{ current_date | replace(current_year, prev_year) }}'

{% set fct_flights = api.Relation.create(
      database="dwh_flight",
      schema="intermediate",
      identifier="fct_flights",
      type="table"
    ) 
%}
{% set stg_flights__facts__flights = api.Relation.create(
      database="dwh_flight",
      schema="intermediate",
      identifier="stg_flights__facts__flights",
      type="table"
    ) 
%}

{% do adapter.expand_target_column_types(stg_flights__facts__flights, fct_flights) %}

{% for column in adapter.get_columns_in_relation(stg_flights__facts__flights) -%}
  {{ "Column: " ~ column }}
{% endfor %}  

{% for column in adapter.get_columns_in_relation(fct_flights) -%}
  {{ "Column: " ~ column }}
{% endfor %}  