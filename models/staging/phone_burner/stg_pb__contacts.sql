with

contact as ( select * from {{ source('phone_burner', 'phoneburner') }} )
,custom_fields as ( select * from {{ source('phone_burner', 'phoneburner_custom_fields') }} where custom_field_id = 575769 )

,renamed as (
    select
        user_id as phone_burner_user_id
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(notes, '$.notes') as string)") }} as notes
        ,owner_id
        ,nullif(latitude,0) as latitude
        ,nullif(longitude,0) as longitude
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_address, '$.address') as string)" ) }} as primary_address_line_1
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_address, '$.address_2') as string)" ) }} as primary_address_line_2
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_address, '$.city') as string)" ) }} as primary_address_city
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_address, '$.state') as string)" ) }} as primary_address_state
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_address, '$.zip') as string)" ) }} as primary_address_postal_code
        ,{{ clean_strings('language') }} as language
        ,null as raw_zip
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_phone, '$.phone') as string)" ) }} as primary_phone_number
        ,{{ clean_strings("cast(JSON_EXTRACT_SCALAR(primary_email, '$.email_address') as string)" ) }} as primary_email_address
        ,contact_user_id
        ,{{ clean_strings('first_name') }} as first_name
        ,{{ clean_strings('last_name') }} as last_name
        ,CASE 
            WHEN custom_fields.custom_field_id = 575769 THEN custom_fields.value
            ELSE NULL 
        END AS user_token
        ,last_call_time as last_call_at_utc
        ,{{ clean_strings('time_zone') }} as contact_timezone
        ,{{ clean_strings('location_name') }} as location_name
        ,{{ clean_strings('region') }} as region
        ,cast(do_not_call as boolean) as is_do_not_call
        ,{{ clean_strings('call_result') }} as call_result
        ,total_calls
        ,TIMESTAMP(date_added) AS date_added_at_utc
        ,TIMESTAMP(date_modified) AS date_modified_at_utc
    from contact
    left join custom_fields on contact.__panoply_id = custom_fields.__phoneburner_panoply_id
)

select * from renamed
