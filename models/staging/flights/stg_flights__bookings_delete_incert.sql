{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['book_ref'],
        tags = ['bookings']
    )

}}
    select
        {{ bookref_to_bigint('book_ref') }} as book_ref_bigint,
        book_date,
        total_amount
    from {{ source('demo_src', 'bookings') }}
{% if is_incremental() %}
where 
    book_date > (SELECT MAX(book_date) FROM {{ source('demo_src', 'bookings') }}) - interval '97 day'
{% endif %}
