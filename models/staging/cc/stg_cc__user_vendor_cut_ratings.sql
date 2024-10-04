with

source as ( select * from {{ source('cc', 'user_vendor_cut_ratings') }} where not _fivetran_deleted )

,renamed as (
    select
        id as user_vendor_cut_rating_id
        ,sku_vendor_id
        ,created_at as created_at_utc
        ,notes --not using clean strings since the original customer comments are useful for marketing
        ,rating
        ,updated_at as updated_at_utc
        ,cut_id
        ,user_id
        ,ai_processed_at as ai_processed_at_utc
        ,ai_notes
        ,ai_sort_order
    from source
)

select * from renamed
