with

sku as ( select * from {{ ref('stg_cc__skus') }} )
,cut as ( select * from {{ ref('stg_cc__cuts') }} )
,farm as ( select * from {{ ref('farms') }} )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )

,sku_joins as (
    select 
        sku.sku_id
        ,sku.sku_key
        ,sku.cut_id
        ,sku.sku_vendor_id
        ,sku.sku_barcode
        ,farm.farm_name
        ,farm.category
        ,farm.sub_category
        ,cut.cut_name
        ,sku.sku_weight
        ,sku.average_cost_usd
        ,sku.platform_fee_usd
        ,sku.fulfillment_fee_usd
        ,sku.payment_processing_fee_usd
        ,sku.standard_price_usd
        ,sku.price_usd
        ,sku.marketplace_cost_usd
        ,sku.average_box_quantity
        ,sku.vendor_funded_discount_name
        ,sku.vendor_funded_discount_usd
        ,sku.promotional_price_usd
        ,sku.member_discount_percent
        ,sku.non_member_discount_percent
        ,sku.is_bulk_receivable
        ,sku.is_presellable
        ,sku.is_virtual_inventory
        ,coalesce(farm.is_cargill,FALSE) as is_cargill
        ,coalesce(farm.is_edm,FALSE) as is_edm
        ,coalesce(sku_vendor.is_marketplace,FALSE) as is_marketplace
        ,sku.vendor_funded_discount_start_at_utc
        ,sku.vendor_funded_discount_end_at_utc
        ,sku.promotion_start_at_utc
        ,sku.promotion_end_at_utc
        ,sku.member_discount_start_at_utc
        ,sku.member_discount_end_at_utc
        ,sku.non_member_discount_start_at_utc
        ,sku.non_member_discount_end_at_utc
        ,sku.active_at_utc
        ,sku.created_at_utc
        ,sku.updated_at_utc
        ,sku.dbt_valid_from
        ,sku.dbt_valid_to
        ,sku.adjusted_dbt_valid_from
        ,sku.adjusted_dbt_valid_to
    from sku
        left join cut on sku.cut_id = cut.cut_id
        left join farm on sku.sku_vendor_id = farm.sku_vendor_id
        left join sku_vendor on sku.sku_vendor_id = sku_vendor.sku_vendor_id
)

select * from sku_joins