{% macro safe_select77(table_name, schema_name=none) %}
  {% set schema = schema_name if schema_name is not none else this.schema %}
  {% set relation = adapter.get_relation(this.database, schema, table_name) %}
  
  {% if relation is not none %}
    
      SELECT * FROM {{ schema }}.{{ table_name }}
   
  {% else %}
    {{ return('SELECT NULL') }}
  {% endif %}
{% endmacro %}