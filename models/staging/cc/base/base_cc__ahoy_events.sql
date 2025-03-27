{{
    config(
        partition_by = {'field': 'time', 'data_type': 'timestamp'},
        cluster_by = ['id','visit_id','user_id','name'],       
)}}


with 
    base_ahoy_events_2025 as ( select * from {{ source('cc', 'ahoy_events') }} where extract(year from time) = 2025 )
-- ,base_ahoy_events_2024 as ( select * from {{ source('cc', 'ahoy_events') }} where  extract(year from time) = 2024 )
--,backup as (select * from raw.ahoy_events_2023) -- as stg_cc__events is an incremental model we can hide this data to reduce query bytes

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
    from base_ahoy_events_2025
   /* union all
    select
        id as event_id
        ,visit_id
        ,name as event_name
        ,row_number() over(partition by visit_id order by time, id) as event_sequence_number
        ,time as occurred_at_utc
        ,updated_at as updated_at_utc
        ,user_id
        ,properties as event_json
    from base_ahoy_events_2024
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
    from backup */ 

)


select * from merged
