

with base_ahoy_events as ( select * from {{ source('cc', 'ahoy_events') }})
,backup as (select * from raw.ahoy_events_2023)

,merged as (
    select
        id as event_id
        ,visit_id
        ,name as event_name
        ,row_number() over(partition by visit_id order by time, id) as event_sequence_number
        ,time as occurred_at_utc
        ,updated_at as updated_at_utc
        ,user_id
        ,properties as event_json
    from base_ahoy_events
    union all 
    select
        id as event_id
        ,visit_id
        ,name as event_name
        ,row_number() over(partition by visit_id order by time, id) as event_sequence_number
        ,time as occurred_at_utc
        ,updated_at as updated_at_utc
        ,user_id
        ,properties as event_json
    from backup

)


select * from merged
