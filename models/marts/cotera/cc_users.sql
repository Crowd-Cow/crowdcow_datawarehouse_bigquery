with

user_details as ( select * from {{ ref('users') }} where not is_rastellis or is_rastellis is null )

select *
from user_details