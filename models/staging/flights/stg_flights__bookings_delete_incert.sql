{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['book_ref'],
        tags = ['bookings']
    )

}}
    select
        book_ref,
        book_date,
        total_amount
    from {{ source('demo_src', 'bookings') }}
{% if is_incremental() %}
where 
    book_date > (SELECT MAX(book_date) FROM {{ source('demo_src', 'bookings') }}) - interval '97 day'
{% endif %}
{% if execute %}
        {% set models = graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | list %}
        {% set seeds = graph.nodes.values() | selectattr('resource_type', 'equalto', 'seed') | list %}
        {% set snapshots = graph.nodes.values() | selectattr('resource_type', 'equalto', 'snapshot') | list %}

        {% set models_count = models | length %}
        {% set seeds_count = seeds | length %}
        {% set snapshots_count = snapshots | length %}

    --- Всего в проекте ---
    -- - {{ models_count }} моделей
    -- - {{ seeds_count }} seed
    -- - {{ snapshots_count }} snapshot

{% endif %}
