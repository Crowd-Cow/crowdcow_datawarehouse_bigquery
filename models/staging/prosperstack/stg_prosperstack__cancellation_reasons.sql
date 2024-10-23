with

source as ( select * from {{ source('prosperstack', 'prosperstack_events') }} )

,renamed as (
    select
        id as event_id
        ,created_at
        ,event as event_name
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(0)], '$.question.text') as question
        ,JSON_EXTRACT_SCALAR(data, '$.cancel_reason.text') as cancelation_reason
        ,JSON_EXTRACT_SCALAR(data, '$.subscriber.platform_id') as user_token
    from raw_prosperstack.prosperstack_events
    where event = 'flow_session_completed'
    order by created_at desc
) 

select * from renamed where question is not null
