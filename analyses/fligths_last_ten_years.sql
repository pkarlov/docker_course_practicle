select
    count(*) as total_flights
FROM    
    {{ ref('fct_flights') }}
where
    scheduled_departure >= '{{ run_started_at | string }}'::date - interval '10 years' and scheduled_departure <= '{{ run_started_at | string }}'
{% set source_relation = adapter.get_relation(
      database="dwh_flight",
      schema="intermediate",
      identifier="fct_flights")
%}
{{ source_relation }}
{{ source_relation.database  }}
{{ source_relation.schema  }}
{{ source_relation.identifier  }}
{{ source_relation.is_table  }}
{{ source_relation.is_view  }}
{{ source_relation.is_cte  }}