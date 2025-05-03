{{
  config(
    materialized = 'table',
    )
}}
with "ticket_cnt_flight" as ( 
SELECT 
count (*) as cnt, flight_id, 
sum (amount) as amount 
FROM
{{ ref('fct_ticket_flights') }} 
group by flight_id
),

"boarding_passes_issued" as (
SELECT 
count (*) as cnt, flight_id
FROM
{{ ref('stg_flights__facts__passes') }} 
group by flight_id
),
"all_seats" as (
SELECT 
aircraft_code,
count (*) as cnt 
FROM
{{ ref('stg_flights__seats') }} 
group by aircraft_code
)





select
    f.flight_id as "Flight_id",
    flight_no as "Flight_no",
    scheduled_departure as "Scheduled_departure_date",
    scheduled_arrival,
    departure_airport as "Departure_Airport_Code",
    arrival_airport as "Arrival_Airport_Code ",
    status as "Flight_status",
    f.aircraft_code as "Aircraft_code",
    actual_departure,
    actual_arrival,
    ad.airport_name as "Departure_Airport_Name",
    ad.city as "Departure_Airport_City",
    ad.coordinates as "Departure_Airport_Coordinates",
    ticket_cnt_flight.cnt as "Ticket_flights_purchased",
    ticket_cnt_flight.amount as "Ticket_flights_amount ",
    boarding_passes_issued.cnt as "Boarding_passes_issued",
    all_seats.cnt - ticket_cnt_flight.cnt as "Ticket_flights_no_sold",
    aa.airport_name as "Arrival_Airport_Coordinates",
    aa.city as "Arrival_Airport_City",
    aa.airport_name as "Arrival_Airport_Name",
    ac.model as "Aircraft_model"
from
    {{ ref('fct_flights') }} as f
left join     
    {{ ref('stg_demo_src__airports') }} as ad
    on f.departure_airport = ad.airport_code
left join   
    ticket_cnt_flight
    on f.flight_id = ticket_cnt_flight.flight_id
left join   
    boarding_passes_issued
    on f.flight_id = boarding_passes_issued.flight_id
left join   
    all_seats
    on f.aircraft_code = all_seats.aircraft_code      
left join     
    {{ ref('stg_demo_src__airports') }} as aa
    on f.arrival_airport = aa.airport_code
left join     
    {{ ref('stg_flights__aircrafts') }} as ac
    on f.aircraft_code = ac.aircraft_code  