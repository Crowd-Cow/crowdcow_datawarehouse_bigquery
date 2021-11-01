with

sku as ( select * from {{ ref('stg_cc__skus') }} where dbt_valid_to is null)
,cut as ( select * from {{ ref('stg_cc__cuts') }} )
,farm as ( select * from {{ ref('farms') }} )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )

,sku_detail as (
    select
        sku_id
        ,cut_id
        ,sku_vendor_id
        ,barcode
        ,sku_name
        ,sku_weight
        ,average_cost_usd
        ,platform_fee_usd
        ,fulfillment_fee_usd
        ,payment_processing_fee_usd
        ,standard_price_usd
        ,price_usd
        ,marketplace_cost_usd
        ,average_box_quantity
        ,vendor_funded_discount_name
        ,vendor_funded_discount_usd
        ,promotional_price_usd
        ,member_discount_percent
        ,non_member_discount_percent
        ,is_bulk_receivable
        ,is_presellable
        ,is_virtual_inventory
        ,vendor_funded_discount_start_at_utc
        ,vendor_funded_discount_end_at_utc
        ,promotion_start_at_utc
        ,promotion_end_at_utc
        ,member_discount_start_at_utc
        ,member_discount_end_at_utc
        ,non_member_discount_start_at_utc
        ,non_member_discount_end_at_utc
        ,active_at_utc
        ,created_at_utc
        ,updated_at_utc
    from sku
)

,sku_joins as (
    select 
        sku_detail.sku_id
        ,sku_detail.cut_id
        ,sku_detail.sku_vendor_id
        ,sku_detail.barcode
        ,farm.farm_name
        ,farm.category
        ,farm.sub_category
        ,cut.cut_name
        ,sku_detail.sku_weight
        ,sku_detail.average_cost_usd
        ,sku_detail.platform_fee_usd
        ,sku_detail.fulfillment_fee_usd
        ,sku_detail.payment_processing_fee_usd
        ,sku_detail.standard_price_usd
        ,sku_detail.price_usd
        ,sku_detail.marketplace_cost_usd
        ,sku_detail.average_box_quantity
        ,sku_detail.vendor_funded_discount_name
        ,sku_detail.vendor_funded_discount_usd
        ,sku_detail.promotional_price_usd
        ,sku_detail.member_discount_percent
        ,sku_detail.non_member_discount_percent
        ,sku_detail.is_bulk_receivable
        ,sku_detail.is_presellable
        ,sku_detail.is_virtual_inventory
        ,coalesce(farm.is_cargill,FALSE) as is_cargill
        ,coalesce(farm.is_edm,FALSE) as is_edm
        ,coalesce(sku_vendor.is_marketplace,FALSE) as is_marketplace
        ,sku_detail.vendor_funded_discount_start_at_utc
        ,sku_detail.vendor_funded_discount_end_at_utc
        ,sku_detail.promotion_start_at_utc
        ,sku_detail.promotion_end_at_utc
        ,sku_detail.member_discount_start_at_utc
        ,sku_detail.member_discount_end_at_utc
        ,sku_detail.non_member_discount_start_at_utc
        ,sku_detail.non_member_discount_end_at_utc
        ,sku_detail.active_at_utc
        ,sku_detail.created_at_utc
        ,sku_detail.updated_at_utc
    from sku_detail
        left join cut on sku_detail.cut_id = cut.cut_id
        left join farm on sku_detail.sku_vendor_id = farm.sku_vendor_id
        left join sku_vendor on sku_detail.sku_vendor_id = sku_vendor.sku_vendor_id
)

select * from sku_joins
