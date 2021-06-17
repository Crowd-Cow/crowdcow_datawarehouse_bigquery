{{
  config(
    tags=["base", "events"]
  )
}}

with base_ahoy_events as (
  select
    ae.id 	    as event_id,
    ae.visit_id,
    ae.name     as event_name,
    ae.time     as occurred_at,
    ae.user_id,
    parse_json(ae.properties) as event_json
  from
    {{ source('cc', 'ahoy_events') }} as ae
)

select * from base_ahoy_events
