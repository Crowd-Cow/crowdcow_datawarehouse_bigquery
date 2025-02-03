with

events as (  select event_id, visit_id, experiments from {{ ref('events') }} where event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION'   

)

,experiment_details as (
    select event_id
        ,visit_id
        ,REGEXP_REPLACE(REGEXP_REPLACE(upper(trim(TO_JSON_STRING(experiments))), r'[\{\}"]', ''),r'EXP-CC-','') as experiments
    from events
)


select *
from experiment_details
