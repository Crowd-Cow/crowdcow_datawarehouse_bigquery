with 
domain_events as (select * from {{ source('cc', 'domain_events') }} where event_type = 'promo_code_used' )

,attempts as (
  select
    created_at as created_at_utc
    ,entity_id 
    ,event_type
    ,id 
    ,user_id
    ,JSON_EXTRACT_SCALAR(data, '$.user_token') AS user_token
    ,JSON_EXTRACT_SCALAR(data, '$.order_token') AS order_token
    ,JSON_EXTRACT_SCALAR(data, '$.result') AS result
    ,JSON_EXTRACT_SCALAR(data, '$.error_report') AS error_report
    ,JSON_EXTRACT_SCALAR(data, '$.code') AS code
  from domain_events
  qualify row_number() over(partition by user_id, order_token order by created_at_utc desc, id desc) = 1
)

select 
    created_at_utc
    ,entity_id 
    ,event_type
    ,id 
    ,user_id
    ,user_token
    ,order_token
    ,result
    ,error_report
    ,{{ clean_strings('code') }} as promo_code 
from attempts