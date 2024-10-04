with

events as ( select * from {{ source('iterable', 'events') }} )

,renamed as (
    select
        __panoply_id as event_id
        ,{{ clean_strings('email') }} as user_email
        ,campaignid as campaign_id
        ,messageid as message_id
        ,contentid as content_id
        ,createdat as created_at_utc
        ,{{ clean_strings('__list_identifier') }} as event_name
        ,ip as ip_address
        ,messagebusid as message_bus_id
        ,{{ clean_strings('recipientstate') }} as recipient_state
        --,status as event_status
        --,unsub_source as unsub_source
        ,{{ clean_strings('useragent') }} as user_agent
        ,{{ clean_strings('useragentdevice') }} as user_agent_device
        ,transactionaldata
        --,additional_properties
        --,is_custom_event
    from events
    where __list_identifier <> 'customEvent'

)

select * from renamed
