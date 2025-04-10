with

cancel_events as ( select * from {{ ref('stg_prosperstack__cancellation_reasons') }}  )

select
    event_id
    ,created_at
    ,event_name
    ,{{ clean_strings('question_1') }} as question_1
    ,{{ clean_strings('sentiment_1') }} as sentiment_1
    ,{{ clean_strings('reason_1') }} as reason_1
    ,{{ clean_strings('question_2') }} as question_2
    ,{{ clean_strings('sentiment_2') }} as sentiment_2
    ,{{ clean_strings('reason_2') }} as reason_2
    ,{{ clean_strings('offers_presented') }} as offers_presented
    ,{{ clean_strings('status') }} as status
    ,user_token

    
from cancel_events 
qualify row_number() over ( partition by user_token order by created_at desc ) = 1
