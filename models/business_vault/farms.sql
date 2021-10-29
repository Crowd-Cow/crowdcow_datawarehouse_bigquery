with

vendor_tags as ( select * from {{ ref('stg_cc__vendor_tags') }} )
,farm_vendor_tags as ( select * from {{ ref('stg_cc__farm_vendor_tags') }} )
,farm as ( select * from {{ ref('stg_cc__farms') }} )

,cargill_farm_tags as (
    select 
        farm_id
        ,vendor_tag_key
        ,vendor_tag_value
    from farm_vendor_tags
        left join vendor_tags on farm_vendor_tags.vendor_tag_id = vendor_tags.vendor_tag_id
    where vendor_tag_key = 'PROVIDER'
        and vendor_tag_value = 'CARGILL'
)

,edm_farm_tags as (
    select 
        farm_id
        ,vendor_tag_key
        ,vendor_tag_value
    from farm_vendor_tags
        left join vendor_tags on farm_vendor_tags.vendor_tag_id = vendor_tags.vendor_tag_id
    where vendor_tag_key = 'EDM'
        and vendor_tag_value
)

,farm_joins as (
    select
        farm.farm_id
        ,farm.farm_token
        ,farm.sku_vendor_id
        ,farm.product_collection_id
        ,farm.price_list_id
        ,farm.farmer_name
        ,farm.farm_name
        ,farm.short_description
        ,farm.email_address
        ,farm.phone_number
        ,farm.city_name
        ,farm.state_name
        ,farm.postal_code
        ,farm.category
        ,farm.sub_category
        ,farm.invoice_payment_terms
        ,farm.meat_type
        ,farm.meat_subtype
        ,farm.capsule_tags
        ,farm.hanging_weight_price_per_pound_usd
        ,farm.slaughter_payment_terms
        ,farm.sort_order
        ,farm.event_title
        ,farm.blog_tags
        ,farm.handwritten_note_nugget
        ,farm.tweetable_nugget
        ,farm.youtube_videos
        ,farm.thanks_video_youtube_token
        ,farm.farm_photo_cached_height
        ,farm.farm_photo_cached_width
        ,farm.website_url
        ,farm.facebook_page_url
        ,farm.photo_url
        ,farm.farmer_photo_url
        ,farm.video_url
        ,farm.thanks_video_static_image_url
        ,coalesce(cargill_farm_tags.farm_id is not null, FALSE) as is_cargill
        ,coalesce(edm_farm_tags.farm_id is not null, FALSE) as is_edm
        ,farm.is_dry_aged
        ,farm.is_zero_antibiotics
        ,farm.is_zero_hormones
        ,farm.is_closed_herd
        ,farm.is_active
        ,farm.is_organic
        ,farm.is_non_gmo
        ,farm.created_at_utc
        ,farm.updated_at_utc
    from farm
        left join cargill_farm_tags on farm.farm_id = cargill_farm_tags.farm_id
        left join edm_farm_tags on farm.farm_id = edm_farm_tags.farm_id
)

select * from farm_joins
