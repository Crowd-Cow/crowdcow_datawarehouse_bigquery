with source as (

    select * from {{ source('cc', 'phone_numbers') }} 

),

renamed as (

  select
     id as phone_number_id
    ,{{ clean_strings('phone_type') }} as phone_type
    ,{{ clean_strings('phone_number') }} as phone_number
    ,token as phone_number_token
    ,if(allow_sms = 1, true, false) as does_allow_sms
    ,created_at as created_at_utc
    ,updated_at as updated_at_utc
    ,validated_at as validated_at_utc

  from source

)

select * from renamed
