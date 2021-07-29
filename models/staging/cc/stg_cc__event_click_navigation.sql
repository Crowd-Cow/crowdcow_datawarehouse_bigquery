{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    tags=["events"]
  )
}}
    
with base as (
  
  select * 
  from {{ ref('base_cc__ahoy_events') }} as ae
  where true 

    {% if is_incremental() %}
      and ae.occurred_at_utc >= coalesce((select max(occurred_at_utc) from {{ this }}), '1900-01-01')
    {% endif %}
    
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
