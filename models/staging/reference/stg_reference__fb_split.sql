with

definitions as ( select * from {{ source('reference_data', 'fb_split') }} )

select
     user_token as user_token
    ,if(rand = 1, "control", "experimental") as fb_test
from definitions

