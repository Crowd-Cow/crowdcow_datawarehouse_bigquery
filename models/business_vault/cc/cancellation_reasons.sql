with

cancel_events as ( select * from {{ ref('stg_prosperstack__cancellation_reasons') }}  )


select
    *
from cancel_events 
qualify row_number() over ( partition by user_token order by created_at desc ) = 1
