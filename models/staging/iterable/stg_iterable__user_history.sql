with

user_hist as ( select * from {{ source('iterable', 'user_history') }} )

,renamed as (
    select
        {{ clean_strings('email') }} as user_email
        ,updated_at as updated_at_utc
        ,{{ clean_strings('first_name') }} as first_name
        ,{{ clean_strings('last_name') }} as last_name
        ,phone_number
        ,user_id as user_token
        ,signup_date as signup_date_utc
        ,{{ clean_strings('signup_source') }} as signup_source
        ,email_list_ids
        ,{{ clean_strings('phone_number_carrier') }} as phone_number_carrier
        ,{{ clean_strings('phone_number_country_code_iso') }} as phone_number_country_code_iso
        ,{{ clean_strings('phone_number_line_type') }} as phone_number_line_type
        ,phone_number_updated_at as phone_number_updated_at_utc
        ,additional_properties
    from user_hist
    qualify row_number() over(partition by email order by updated_at desc) = 1
)

select * from renamed
