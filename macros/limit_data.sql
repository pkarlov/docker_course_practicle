{% macro limit_data(column_name, rows = 1000) %}
{% if target.name == 'dev' %}
order by {{column_name}} desc
limit {{rows}}
{% endif %}
{% endmacro %}