{{
    config(
        severity = 'error',
        error_if = '>1500000',
        warn_if = '>1470000'
    )
}}

select
    b.book_ref
from
    {{ ref('stg_flights__bookings') }} b
    join {{ ref('stg_flights__facts__tickets') }} t
    on b.book_ref = t.book_ref

where
    length(b.book_ref) > 7   