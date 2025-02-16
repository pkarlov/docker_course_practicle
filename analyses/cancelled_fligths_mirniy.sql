SELECT 
    scheduled_departure::date as scheduled_departure,
    count(*) as cancelled_fligths_cnt
FROM
    {{ ref('fct_flights') }}  
where
    departure_airport = 'MJZ'
    AND status = 'Cancelled'
GROUP BY
    scheduled_departure::date    
--MJZ    