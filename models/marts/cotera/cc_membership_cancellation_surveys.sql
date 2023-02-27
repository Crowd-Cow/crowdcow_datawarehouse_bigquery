
with member_cancellations as ( select * from {{ ref('cancellation_surveys') }} )

select *
from member_cancellations