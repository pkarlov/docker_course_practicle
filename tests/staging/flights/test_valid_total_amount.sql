SELECT *
FROM {{ ref('stg_flights__bookings') }}
WHERE total_amount <= 0 OR total_amount > 10000000
