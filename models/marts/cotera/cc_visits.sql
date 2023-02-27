with

visit_details as ( select * from {{ ref('visits') }} )

select *
from visit_details