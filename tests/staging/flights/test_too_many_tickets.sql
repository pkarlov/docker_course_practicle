{{
  config(
    severity = 'warn',
    error_if = '> 100',
    warn_if = '>= 50'
  )
}}

WITH ticket_counts AS (
    SELECT
        book_ref,
        COUNT(*) AS ticket_count
    FROM {{ ref('stg_flights__facts__tickets') }}
    GROUP BY book_ref
)
SELECT
    book_ref
FROM ticket_counts
WHERE ticket_count >= 5