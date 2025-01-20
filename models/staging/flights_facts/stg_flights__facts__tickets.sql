{{
  config(
    materialized = 'table'
    )
}}
select
    ticket_no,
    book_ref,
    passenger_id,
    passenger_name,
    contact_data

from {{ source('demo_src', 'tickets') }}
--{{ limit_data(column_name = 'ticket_no', rows = 1000) }}