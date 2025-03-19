{{
  config(
    materialized = 'table',
    meta = {
      'owner': 'sql_file_owner@gmail.com'
    }
  )
}}
SELECT 
    {{ show_columns_relation('stg_flights__bookings') }}
FROM {{ ref('stg_flights__bookings') }}