with

member_details as ( select * from {{ ref('memberships') }} )

select *
from member_details