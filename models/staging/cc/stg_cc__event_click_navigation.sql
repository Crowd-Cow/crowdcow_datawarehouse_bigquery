{{
    config(
        tags = ["events"]
    )
}}

with base as (

    select * from {{ ref('base_cc__ahoy_events') }}
),

event_click_navigation as (

    select
        event_id
        ,visit_id
        ,occurred_at_utc
        ,user_id
        ,trim({{ clean_strings('event_json:label::text') }},'\n') as navigation_label
        ,event_json:member::boolean as is_member
    from
        base
    where
        event_name = 'custom_event'
            and event_json:category::text = 'navigation'
            and event_json:action::text = 'click-navigation'

)

select * from event_click_navigation
