{{
  config(
    tags=["base", "events"]
  )
}}

with base_ahoy_events as (
  select
     id        as event_id
    ,visit_id
    ,name      as event_name
    ,time      as occurred_at_utc
    ,user_id
    ,parse_json(properties) as event_json
  from
    {{ source('cc', 'ahoy_events') }}
  where not _fivetran_deleted
)

select * from base_ahoy_events
