{%- macro concat_columns(columns, delim = ', ') %}
    {%- for column in columns -%}
        {{ column }} {% if not loop.last %} || '{{ delim }}' || {% endif %}
    {%- endfor -%}
{% endmacro %}


{% macro drop_old_relations(dryrun=False) %}

    {% if execute %}
    
        {# находим все модели, seed, snapshot проекта dbt #}
        
        {% set current_models = [] %}
        
        {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "snapshot", "seed"]) %}
            {% do current_models.append(node.name) %}
        {% endfor %}
        
        {# формирование скрипта удаления всез таблиц и вьюх, которым не соответствует ни одна модель, сид и снэпшот #}
        
        {% set cleanup_query %}
        WITH MODELS_TO_DROP AS (
            SELECT
                CASE
                    WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
                    WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
                END AS RELATION_TYPE,
                CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) as RELATION_NAME
            FROM
                {{ target.database }}.INFORMATION_SCHEMA.TABLES
            WHERE
                TABLE_SCHEMA = '{{ target.schema }}'
                AND UPPER(TABLE_NAME) NOT IN (
                    {%- for model in current_models -%}
                        '{{ model.upper() }}'
                        {%- if not loop.last -%}
                            ,
                        {%- endif %}
                    {%- endfor -%}
                )
        )
        SELECT
            'DROP ' || RELATION_TYPE || ' ' || RELATION_NAME || ';' as DROP_COMMANDS
        FROM
            MODELS_TO_DROP;
        {% endset %}
        
        {% do log(cleanup_query) %}
        
        {% set drop_commands = run_query(cleanup_query).columns[0].values() %}
        
        {# удаление лишних таблиц и вьюх / вывод скрипта удаления #}
        
        
        {% if drop_commands %}
            {% if dryrun | as_bool == False %}
                {% do log('Executing DROP commands ...', True) %}
            {% else %}
                {% do log('Printing DROP commands ...', True) %}
            {% endif %}
        
            {% for drop_command in drop_commands %}
                {% do log(drop_command, True) %}
                {% if  dryrun | as_bool == False %}
                    {% do run_query(drop_command) %}
                {% endif %}
            {% endfor %}
        {% else %}
             {% do log('No relations to clean', True) %}
        {% endif %}
    
    {% endif %}

{% endmacro %}


{% macro bookref_to_bigint(bookref) %}
('0x' || {{ bookref }})::bigint
{% endmacro %}

{% macro safe_select(table_name) %}
  {%- set relation = adapter.get_relation(this.database, this.schema, table_name) -%}
  
  {%- if relation is not none -%}
    SELECT * FROM {{ table_name }}
  {%- else -%}
    SELECT NULL
  {%- endif -%}
{% endmacro %}

{% macro safe_select3(table_ref) %}
  {% if table_ref is string %}
    {# Если передана строка #}
    {% set relation = adapter.get_relation(this.database, this.schema, table_ref) %}
    {% set table_identifier = table_ref %}
  {% else %}
    {# Если передан объект Relation #}
    {% set relation = table_ref %}
    {% set table_identifier = table_ref %}
  {% endif %}
  
  {% if relation is not none %}
    SELECT * FROM {{ table_identifier }}
  {% else %}
    SELECT NULL
  {% endif %}
{% endmacro %}

{% macro show_columns_relation(model_name) %}
  {% set relation = ref(model_name) %}
  {% set columns = adapter.get_columns_in_relation(relation) %}
  
  {% set column_names = [] %}
  
  {%- for column in columns -%}
    {% do column_names.append(column.name) %}
  {%- endfor -%}
  
  {%- for column_name in column_names -%}
    {{ column_name }}{{ ", " if not loop.last }}
  {%- endfor -%}
{% endmacro %}

{% macro safe_select5(table_name) %}
  {%- set relation = adapter.get_relation(this.database, this.schema, table_name) -%}
  
  {%- if relation is not none -%}
    {%- do return('SELECT * FROM ' ~ table_name) -%}
  {%- else -%}
    {%- do return('SELECT NULL') -%}
  {%- endif -%}
{% endmacro %}

{% macro check_dependencies1() %}
    {% set dependent_objects_count = 0 %}

    {% for source in this.depends_on %}
        
            {% set dependent_objects_count = dependent_objects_count + 1 %}
        
    {% endfor %}

    {% if dependent_objects_count > 0 %}
        ⚠️ Модель {{ this.name }} зависит от {{ dependent_objects_count }} объектов!
        {% do exceptions.warn("⚠️ Модель " ~ this.name ~ " зависит от " ~ dependent_objects_count ~ " объектов!" ) %}
    {% endif %}
{% endmacro %}


{% macro check_dependencies() %}

{# Получаем текущую модель #}
{% set current_model = model %}

{# Получаем все зависимости текущей модели #}
{% set dependencies = current_model.depends_on.nodes %}

{# Считаем количество зависимостей #}
{% set dependency_count = dependencies | length %}

{# Если зависимостей больше одной, выводим предупреждение #}
{% if dependency_count > 1 %}
    {{ log("⚠️ Model " ~ current_model.name ~ " depend on " ~ dependency_count ~ " objects!", info=True) }}
    {% do exceptions.warn("⚠️ Model " ~ current_model.name ~ " depend on " ~ dependency_count ~ " objects!" ) %}
{% endif %}

{% endmacro %}

{% macro check_dependencies2(model) %}
    {% set dependencies = model.depends_on %}
    {% set dependencies_count = dependencies|length %}

    {% do log("Количество зависимостей модели " ~ model.name ~ ": " ~ dependencies_count, info=True) %}

    {% if dependencies_count > 1 %}
        {% do exceptions.warn("⚠️ Модель " ~ current_model.name ~ " зависит от " ~ dependency_count ~ " объектов!" ) %}
        {% do log("⚠️ Модель " ~ model.name ~ " зависит от " ~ dependencies_count ~ " объектов!", info=True) %}
    {% endif %}
{% endmacro %}

{% macro check_dependencies3(model) %}
    {% do log("DEBUG: depends_on = " ~ model.depends_on, info=True) %}
{% endmacro %}