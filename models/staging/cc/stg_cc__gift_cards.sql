with source as (

    select * from {{ source('cc', 'gift_cards') }} where not _fivetran_deleted

),

renamed as (

    select
        id as gift_card_id
        ,redeemed_at as redeemed_at_utc
        ,{{ clean_strings('vendor') }} as gift_card_vendor
        ,created_at as created_at_utc
        ,{{ clean_strings('batch_uuid') }} as batch_uuid
        ,admin_id
        ,gift_info_id
        ,frauded_at as frauded_at_utc
        ,{{ clean_strings('suspicion_reason') }} as suspicion_reason
        ,{{ cents_to_usd('amount_in_cents') }} as gift_card_amount_usd
        ,suspicion_found_at as suspicion_found_at_utc
        ,user_id
        ,suspicion_resolved_at as suspicion_resolved_at_utc
        ,{{ clean_strings('amount_frauded_in_cents') }} as gift_card_fraud_amount_usd
        ,{{ clean_strings('image_url') }} as image_url
        ,{{ clean_strings('code') }} as gift_card_code
        ,updated_at as updated_at_utc
        ,sold_at as sold_at_utc

    from source

)

select * from renamed

