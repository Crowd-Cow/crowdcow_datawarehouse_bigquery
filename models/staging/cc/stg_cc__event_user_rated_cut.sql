{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_user_rated_cut as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments         as experiments
    ,event_json:member::boolean     as is_member
    ,event_json:cut_id::int         as cut_id
    ,event_json:notes::text         as notes
    ,event_json:rating::int         as rating
    ,event_json:sku_vendor_id::int  as sku_vendor_id
  from 
    base
  where 
    event_name = 'user_rated_cut'

)

select * from event_user_rated_cut
