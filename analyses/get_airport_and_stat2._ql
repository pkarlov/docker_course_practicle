{%- set statuses = dbt_utils.get_column_values(
    table=ref('fct_flights'),
    column='status'
) -%}

select
departure_airport,
{%- for status in statuses -%}
sum(case when status = '{{ status }}' THEN 1 ELSE 0 END) as {{ dbt_utils.slugify(status) -}} 
{%- if not loop.last %},{% endif %}
{% endfor -%}
FROM
{{ ref('fct_flights') }}
group by departure_airport

{{ dbt_utils.pivot('departure_airport', get_column_values(ref('fct_flights'))) }}
{{ dbt_utils.safe_subtract(['departure_airport']) }}