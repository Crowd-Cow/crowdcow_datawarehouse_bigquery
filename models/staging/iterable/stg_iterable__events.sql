with

events as ( select * from {{ source('iterable', 'event') }} )

,renamed as (
    select
        {{ clean_strings('email') }} as user_email
        ,campaign_id
        ,message_id
        ,content_id
        ,created_at as created_at_utc
        ,{{ clean_strings('event_name') }} as event_name
        ,ip as ip_address
        ,message_bus_id
        ,{{ clean_strings('recipient_state') }} as recipient_state
        ,{{ clean_strings('status') }} as event_status
        ,{{ clean_strings('unsub_source') }} as unsub_source
        ,{{ clean_strings('user_agent') }} as user_agent
        ,{{ clean_strings('user_agent_device') }} as user_agent_device
        ,transactional_data
        ,additional_properties
        ,is_custom_event
    from events
    where not is_custom_event
)

select * from renamed
