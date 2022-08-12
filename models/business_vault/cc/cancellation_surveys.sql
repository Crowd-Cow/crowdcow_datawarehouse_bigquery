with

cancel_events as ( select * from {{ ref('stg_cc__events') }} where event_name = 'BRIGHTBACK_CANCEL' )

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
from cancel_events 
qualify row_number() over ( partition by brightback_id order by occurred_at_utc desc ) = 1
