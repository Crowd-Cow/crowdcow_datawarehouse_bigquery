with

source as ( select * from {{ ref('stg_cc__customer_feedback_summaries') }} )

,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,cut as ( select * from {{ ref('stg_cc__cuts') }} )

,get_cut_key as (
    select
        source.*
        ,cut.cut_key
    from source
    left join cut on source.cut_id = cut.cut_id
        and source.created_at_utc >= cut.adjusted_dbt_valid_from
        and source.created_at_utc < cut.adjusted_dbt_valid_to
)

,get_vendor_name as (
    select
        get_cut_key.*
        ,sku_vendor.sku_vendor_name
    from get_cut_key
        left join sku_vendor on get_cut_key.sku_vendor_id = sku_vendor.sku_vendor_id
)

,renamed as (
    select
        customer_feedback_summaries_id
        ,sku_vendor_id
        ,cut_id
        ,lot_id
        ,order_id
        ,created_at_utc
        ,feedback_summary_of_id
        ,feedback_summary_of_type
        ,zendesk_id
        ,rating_id
        ,overall_rating
        ,arrived_frozen
        ,butchering_quality
        ,color_appearance
        ,delivery_issue
        ,likelyness_to_reorder
        ,packaging
        ,price_and_value
        ,product_age
        ,product_weight
        ,received_wrong_item
        ,review_summary
        ,smell
        ,taste
        ,texture
        ,website_issue
        ,customer_feedback_updated_at_utc
        ,sku_vendor_name
        ,cut_key

    from get_vendor_name
)

select * from renamed