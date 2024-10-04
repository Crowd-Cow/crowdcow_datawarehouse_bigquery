with

contact as ( select * from {{ ref('stg_pb__contacts') }} )

,map_owner_name as (
    select
        phone_burner_user_id
        ,notes
        ,owner_id
        ,if(owner_id = 778572808,'STEPHANIE GRACE','DANIEL ALEJANDRE') as owner_name
        ,latitude
        ,longitude
        ,primary_address_line_1
        ,primary_address_line_2
        ,primary_address_city
        ,primary_address_state
        ,primary_address_postal_code
        ,language
        ,raw_zip
        ,primary_phone_number
        ,primary_email_address
        ,contact_user_id
        ,first_name
        ,last_name
        ,user_token
        ,last_call_at_utc
        ,contact_timezone
        ,location_name
        ,region
        ,is_do_not_call
        ,call_result
        ,total_calls
        ,date_added_at_utc
        ,date_modified_at_utc
    from contact
)

select * from map_owner_name
