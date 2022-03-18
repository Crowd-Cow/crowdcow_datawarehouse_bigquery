with

contact as ( select * from {{ source('phone_burner', 'contacts') }} )

,renamed as (
    select
        user_id as phone_burner_user_id
        ,{{ clean_strings('notes:notes::text') }} as notes
        ,owner_id
        ,nullif(latitude,0) as latitude
        ,nullif(longitude,0) as longitude
        ,{{ clean_strings('primary_address:address::text') }} as primary_address_line_1
        ,{{ clean_strings('primary_address:address_2::text') }} as primary_address_line_2
        ,{{ clean_strings('primary_address:city::text') }} as primary_address_city
        ,{{ clean_strings('primary_address:state::text') }} as primary_address_state
        ,{{ clean_strings('primary_address:zip::text') }} as primary_address_postal_code
        ,{{ clean_strings('language') }} as language
        ,{{ clean_strings('raw_zip') }} as raw_zip
        ,{{ clean_strings('primary_phone:phone::text') }} as primary_phone_number
        ,{{ clean_strings('primary_email:email_address::text') }} as primary_email_address
        ,contact_user_id
        ,{{ clean_strings('first_name') }} as first_name
        ,{{ clean_strings('last_name') }} as last_name
        ,iff(custom_fields[0]:custom_field_id::int = 575769,custom_fields[0]:value::text,null) as user_token
        ,convert_timezone('America/Chicago','UTC',nullif(last_call_time,'0')) as last_call_at_utc
        ,{{ clean_strings('time_zone') }} as contact_timezone
        ,{{ clean_strings('location_name') }} as location_name
        ,{{ clean_strings('region') }} as region
        ,do_not_call::boolean as is_do_not_call
        ,{{ clean_strings('call_result') }} as call_result
        ,total_calls
        ,convert_timezone('America/Chicago','UTC',date_added) as date_added_at_utc
        ,convert_timezone('America/Chicago','UTC',date_modified) As date_modified_at_utc
    from contact
)

select * from renamed
