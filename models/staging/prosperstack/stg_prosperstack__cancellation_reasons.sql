with

source as ( select * from {{ source('prosperstack', 'prosperstack_events') }} )

,renamed as (
    select
        id as event_id
        ,created_at
        ,event as event_name
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(0)], '$.question.text') as question_1
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(0)], '$.sentiment') as sentiment_1
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(0)], '$.value[0].text') as reason_1
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(1)], '$.question.text') as question_2
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(1)], '$.sentiment') as sentiment_2
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.answers')[SAFE_OFFSET(1)], '$.value') as reason_2
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(data,'$.offers_presented')[SAFE_OFFSET(0)], '$.name') as offers_presented
        ,JSON_EXTRACT_SCALAR(data, '$.status') as status
        ,JSON_EXTRACT_SCALAR(data, '$.subscriber.platform_id') as user_token
    from raw_prosperstack.prosperstack_events
    where event = 'flow_session_completed'
    order by created_at desc
) 

select * from renamed where question_1 is not null
