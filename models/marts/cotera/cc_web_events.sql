with

event_details as ( select * from {{ ref('events') }} )

select *
from event_details