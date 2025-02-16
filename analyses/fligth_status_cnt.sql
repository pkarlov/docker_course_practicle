{% set status_query %}
SELECT DISTINCT
    status
FROM
    {{ ref('stg_flights__facts__flights') }}
{%endset%}        

{% set status_query_result = run_query(status_query) %}
{% if execute %}

{% set important_status = status_query_result.columns[0].values() %}
{% else %}
{% set important_status = [] %}
{% endif %}

SELECT DISTINCT
    {%- for status in important_status %}
    SUM(CASE WHEN status = '{{ status }}' THEN 1 ELSE 0 END) as "status_{{ status }}"
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
FROM
    {{ ref('stg_flights__facts__flights') }}
