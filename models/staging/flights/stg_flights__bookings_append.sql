{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append', 
        tags = ['bookings'],
    )
}}
SELECT
    book_ref,
    book_date,
    {{ kopeck_to_ruble('total_amount', -2) }} as total_amount
FROM
    {{ source('demo_src', 'bookings') }}
{% if is_incremental() %}
WHERE 
    ('0x' || book_ref)::bigint > (SELECT MAX(('0x' || book_ref)::bigint) FROM {{ this }})
{% endif %}
{% if execute %}
-- {{ graph.nodes.values() }}
  {% for node in graph.nodes.values() -%}
    {% if node.resource_type == 'model' or node.resource_type == 'seed' %}
-- {{ node.name }}
---------------------
-- {{ node.depends_on }}
---------------------
    {% endif %}
  {% endfor %}
{% endif %}