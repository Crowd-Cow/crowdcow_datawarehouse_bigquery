with

failure_case as ( select * from {{ ref('stg_cc__failure_cases') }} )

select 
    *
    ,case 
            when specifics like any ('%QUALITY%') then 'PRODUCT QUALITY'
            when specifics like any ('%MISSING ITEM%') then 'MISSING ITEM'
            when specifics like any ('%CUSTOMER SERVICE%','%SUPPORT%') then 'CUSTOMER SERVICE'
            when specifics like any ('%DELIVERY%LATE%','%SHIPMENT%LATE%','%NEVER%ARRIVED%','%LOST%') then 'DELIVERY LATE'
            when specifics like any ('%LEAK%') then 'LEAKER'
            when specifics like any ('%RESCHEDULE%') or category = 'RESCHEDULED ORDER' then 'RESCHEDULE'
            when specifics like any ('%THAW%') then 'THAW'
            when specifics like any ('%REMOVED%','%NO%INVENTORY%') then 'REMOVED ITEMS'
            else 'OTHER'
        end as standard_category
        
from failure_case
