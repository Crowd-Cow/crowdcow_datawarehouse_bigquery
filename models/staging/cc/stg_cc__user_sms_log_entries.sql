with

source as ( select * from {{ source('cc', 'sms_log_entries') }} )

,renamed as (
    select  
        id as sms_log_id
        ,actually_sent_at as actually_sent_at_utc
        ,first_clicked_at as first_clicked_at_utc
        ,from_number
        ,to_number
        ,order_id
        ,shipment_id 
        ,key
        ,from_type
        ,to_user_id
        ,{{ clean_strings('message') }} as message
    from source
)

select * from renamed
