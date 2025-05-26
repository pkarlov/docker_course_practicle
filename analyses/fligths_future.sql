select
    count(*) as total_flights
from
    {{ ref('fct_flights') }}
where
    scheduled_departure >= '{{ run_started_at | string }}'