{% macro limit_data_dev(column_name, days=5000) %}
    {% if days < 0 %}
        {% do exceptions.warn("Invalid `days`. Got: " ~ days) %}
    {% endif %}
{% if target.name == 'dev' %}
WHERE
    --{{ column_name }} >= current_date - interval '{{ days }} days'
    {{ column_name }} >= {{ dbt.dateadd(datepart="day", interval=-days, from_date_or_timestamp="current_date") }}
{% endif %}
{% endmacro %}