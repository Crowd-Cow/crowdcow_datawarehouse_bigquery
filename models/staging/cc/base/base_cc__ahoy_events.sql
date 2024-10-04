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
    ,row_number() over(partition by visit_id order by time, id) as event_sequence_number
    ,time      as occurred_at_utc
    ,updated_at as updated_at_utc
    ,user_id
    ,parse_json(properties) as event_json
  from
    {{ source('cc', 'ahoy_events') }}
  where not _fivetran_deleted
)

select * from base_ahoy_events
