with

rating as ( select * from {{ ref('stg_cc__user_vendor_cut_ratings') }} )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,cut as ( select * from {{ ref('stg_cc__cuts') }} )

,get_cut_key as (
    select
        rating.*
        ,cut.cut_key
    from rating
    left join cut on rating.cut_id = cut.cut_id
        and rating.created_at_utc >= cut.adjusted_dbt_valid_from
        and rating.created_at_utc < cut.adjusted_dbt_valid_to
)

,get_vendor_name as (
    select
        get_cut_key.*
        ,sku_vendor.sku_vendor_name
    from get_cut_key
        left join sku_vendor on get_cut_key.sku_vendor_id = sku_vendor.sku_vendor_id
)

select * from get_vendor_name
