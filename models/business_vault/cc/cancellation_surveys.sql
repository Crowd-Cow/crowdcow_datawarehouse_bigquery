{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
        cluster_by = ['event_id','visit_id','user_id'],
        incremental_strategy = 'insert_overwrite',
        partitions = partitions_to_replace
    )
}}
with

cancel_events as ( 
    select 
    event_id
    ,visit_id
    ,user_id
    ,user_token
    ,subscription_token
    ,occurred_at_utc
    ,brightback_id
    ,session_id
    ,session_key
    ,display_reason
    ,feedback
    ,selected_reason
    ,sentiment
    from {{ ref('stg_cc__events') }} 
    where event_name = 'BRIGHTBACK_CANCEL' 
    {% if is_incremental() %}
     and timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    
    )


select
    event_id
    ,visit_id
    ,user_id
    ,user_token
    ,subscription_token
    ,occurred_at_utc
    ,brightback_id
    ,session_id
    ,session_key
    ,display_reason
    ,feedback
    ,selected_reason
    ,sentiment
    ,IF(
        feedback IS NOT NULL,
        {{ process_text('feedback') }},
        NULL
    ) AS clean_feedback
from cancel_events 
qualify row_number() over ( partition by brightback_id order by occurred_at_utc desc ) = 1
