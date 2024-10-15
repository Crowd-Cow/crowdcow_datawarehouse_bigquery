{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
        cluster_by = ['visit_id','user_id','event_name'],
        incremental_strategy = 'insert_overwrite',
        partitions = partitions_to_replace
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
    ,properties as event_json
  from
    {{ source('cc', 'ahoy_events') }}
  {% if is_incremental() %}
     where timestamp_trunc(time, day) in ({{ partitions_to_replace | join(',') }})
  {% endif %}
)

select * from base_ahoy_events
