select
    count(*) as total_flights
FROM    
    {{ ref('fct_flights') }}
where
    scheduled_departure >= '{{ run_started_at | string }}'::date - interval '10 years' and scheduled_departure <= '{{ run_started_at | string }}'