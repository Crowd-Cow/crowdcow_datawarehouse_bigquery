{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_product_search as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments                 as experiments
    ,event_json:member::boolean             as is_member
    ,event_json:search_hit_count::int       as search_hit_count
    ,event_json:search_terms::text          as search_terms
    ,event_json:spelling_suggestion::text   as spelling_suggestion
  from 
    base
  where 
    event_name = 'product_search'

)

select * from event_product_search
