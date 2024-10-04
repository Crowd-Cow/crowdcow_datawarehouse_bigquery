with source as (

    select * from {{ source('cc', 'gift_infos') }} where not _fivetran_deleted

),

renamed as (

    select
        id as gift_info_id
        ,{{ clean_strings('recipient_email') }} as recipient_email
        ,order_id
        ,suggested_product_id
        ,token as gift_info_token
        ,updated_at as updated_at_utc
        ,frauded_at as frauded_at_utc
        ,{{ clean_strings('recipient_name') }} as recipient_name
        ,{{ clean_strings('sender_email') }} as sender_email
        ,created_at as created_at_utc
        ,{{ clean_strings('gift_message') }} as gift_message
        ,{{ clean_strings('sender_name') }} as sender_name
        ,{{ clean_strings('delivery_method') }} as delivery_method
        ,suggested_quantity
        ,suggested_bid_item_id
        ,{{ clean_strings('image_url') }} as image_url
        ,is_surprise

    from source

)

select * from renamed

