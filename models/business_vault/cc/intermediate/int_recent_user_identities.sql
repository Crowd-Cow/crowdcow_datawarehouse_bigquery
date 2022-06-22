with

identity as ( select * from {{ ref('stg_cc__identities') }} )

select
    user_id
    ,first_name
    ,last_name
    ,full_name
from identity
where user_id is not null
qualify row_number() over(partition by user_id order by updated_at_utc desc) = 1
