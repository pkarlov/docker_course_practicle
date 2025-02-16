SELECT 
    aircraft_code,
    count(*)
FROM
    {{ ref('stg_flights__seats') }}  
group BY
    aircraft_code