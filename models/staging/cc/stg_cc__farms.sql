with source as (

    select * from {{ ref('farms_ss') }} where not _fivetran_deleted

),

renamed as (

    select
        id as farm_id
        , dbt_scd_id as farm_key
        , capsule_tags
        , {{ cents_to_usd('hanging_weight_price_per_pound_cents') }} as hanging_weight_price_per_pound_usd
        , slaughter_payment_terms
        , {{ clean_strings('photo_url') }} as photo_url
        , {{ clean_strings('farmer_name') }} as farmer_name
        , {{ clean_strings('category') }} as category
        , invoice_payment_terms
        , created_at as created_at_utc
        , {{ clean_strings('meat_subtype') }} as meat_subtype
        , {{ clean_strings('name') }} as farm_name
        , youtube_videos
        , farm_photo_cached_height
        , sku_vendor_id
        , blog_tags
        , {{ clean_strings('subcategory') }} as sub_category
        , token as farm_token
        , {{ clean_strings('farmer_photo_url') }} as farmer_photo_url
        , farm_photo_cached_width
        , {{ clean_strings('twitter_handle') }} as twitter_handle
        , {{ clean_strings('email_address') }} as email_address
        , {{ clean_strings('handwritten_note_nugget') }} as handwritten_note_nugget
        , postal_code
        , {{ clean_strings('video_url') }} as video_url
        , product_collection_id
        , sort_order
        , {{ clean_strings('event_title') }} as event_title
        , price_list_id
        , phone_number
        , {{ clean_strings('city_name') }} as city_name
        , {{ clean_strings('tweetable_nugget') }} as tweetable_nugget
        , {{ clean_strings('meat_type') }} as meat_type
        , {{ clean_strings('thanks_video_static_image_url') }}  as thanks_video_static_image_url
        , {{ clean_strings('state_name') }} as state_name
        , {{ clean_strings('website_url') }} as website_url
        , {{ clean_strings('facebook_page_url') }} as facebook_page_url
        , updated_at as updated_at_utc
        , {{ clean_strings('short_description') }}  as short_description
        , thanks_video_youtube_token
        , is_dry_aged
        , is_zero_antibiotics
        , is_zero_hormones
        , is_closed_herd
        , active as is_active
        , is_organic
        , is_non_gmo
        , dbt_valid_to
        , dbt_valid_from
        , case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        , coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed

