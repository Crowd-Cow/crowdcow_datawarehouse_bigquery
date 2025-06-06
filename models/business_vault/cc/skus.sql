with

sku as ( select * from {{ ref('stg_cc__skus') }} )
,cut as ( select * from {{ ref('stg_cc__cuts') }} )
,farm as ( select * from {{ ref('farms') }} )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,ais as ( select * from {{ ref('stg_gs__always_in_stock') }} )
,inventory_classification as ( select * from {{ ref('stg_gs__inventory_classification') }} )
,standard_sku_cost as ( select * from {{ ref('stg_gs__standard_sku_cost') }} )

,sku_joins as (
    select 
        sku.sku_id
        ,sku.sku_key
        ,sku.cut_id
        ,cut.cut_key
        ,sku.sku_vendor_id
        ,{{ dbt_utils.surrogate_key(['farm.category','farm.sub_category','cut.cut_name','sku.sku_name']) }} as ais_id
        ,{{ dbt_utils.surrogate_key(['farm.category','farm.sub_category','cut.cut_name']) }} as inventory_classification_id
        ,sku.sku_barcode
        ,farm.farm_id
        ,farm.farm_name
        ,farm.is_active as is_active_farm
        ,farm.category
        ,case when  sku.sku_vendor_id in (272, 223, 296) then 'DOMESTIC PASTURE RAISED' 
              when  sku.sku_vendor_id = 307 then '100% DOMESTIC GRASS FED'
              else farm.sub_category end as sub_category
        ,cut.cut_name
        ,sku.sku_name
        ,sku.sku_weight
        ,sku.owned_sku_cost_usd
        ,sku.marketplace_cost_usd
        ,sku.platform_fee_usd
        ,sku.fulfillment_fee_usd
        ,sku.payment_processing_fee_usd
        ,sku.standard_price_usd
        ,sku.sku_price_usd
        ,sku.average_box_quantity
        ,sku.vendor_funded_discount_name
        ,sku.vendor_funded_discount_usd
        ,sku.vendor_funded_discount_percent
        ,sku.promotional_price_usd
        ,sku.member_discount_percent
        ,sku.non_member_discount_percent
        ,sku.replenishment_code
        ,sku.vendor_case_pack_quantity
        ,sku.vendor_product_code
        ,sku.is_bulk_receivable
        ,sku.is_presellable
        ,sku.is_virtual_inventory
        ,coalesce(farm.is_cargill,FALSE) as is_cargill
        ,coalesce(farm.is_edm,FALSE) as is_edm
        ,coalesce(sku_vendor.is_marketplace,FALSE) as is_marketplace
        ,coalesce(sku_vendor.is_rastellis,FALSE) as is_rastellis
        ,sku_vendor.sku_vendor_name
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
            and sku.created_at_utc >= cut.adjusted_dbt_valid_from
            and sku.created_at_utc < cut.adjusted_dbt_valid_to
        left join farm on sku.sku_vendor_id = farm.sku_vendor_id
            and farm.dbt_valid_to is null
        left join sku_vendor on sku.sku_vendor_id = sku_vendor.sku_vendor_id
)

,add_inventory_classification as (
    select
        sku_joins.sku_id
        ,sku_joins.sku_key
        ,sku_joins.cut_id
        ,sku_joins.cut_key
        ,sku_joins.sku_vendor_id
        ,sku_joins.sku_barcode
        ,sku_joins.farm_id
        ,sku_joins.farm_name
        ,sku_joins.is_active_farm
        ,sku_joins.category
        ,sku_joins.sub_category
        ,sku_joins.cut_name
        ,sku_joins.sku_name
        
        /** Historical SKU snapshots did not include weight. Filling in a default value of average SKU weight if not provided. **/
        ,case
            when sku_joins.sku_weight is null then avg(sku_weight) over(partition by sku_id)
            else sku_joins.sku_weight
         end as sku_weight
        
        /** Accounts for $0 cost SKUs by taking the average cost for all snapshots of a sku if the cost was entered as $0 in Inventory Finances **/
        ,coalesce(
            case
                when sku_joins.owned_sku_cost_usd = 0 then avg(nullif(sku_joins.owned_sku_cost_usd,0)) over(partition by sku_joins.sku_id) 
                else sku_joins.owned_sku_cost_usd
            end
        ) as owned_sku_cost_usd

        ,sku_joins.marketplace_cost_usd
        ,if(sku_joins.is_marketplace,sku_joins.marketplace_cost_usd,sku_joins.owned_sku_cost_usd) as sku_cost_usd
        ,sku_joins.platform_fee_usd
        ,sku_joins.fulfillment_fee_usd
        ,sku_joins.payment_processing_fee_usd
        ,sku_joins.standard_price_usd
        ,sku_joins.sku_price_usd
        ,sku_joins.average_box_quantity
        ,sku_joins.vendor_funded_discount_name
        ,sku_joins.vendor_funded_discount_usd
        ,sku_joins.vendor_funded_discount_percent
        ,sku_joins.promotional_price_usd
        ,sku_joins.member_discount_percent
        ,sku_joins.non_member_discount_percent
        ,sku_joins.replenishment_code
        ,sku_joins.vendor_case_pack_quantity
        ,sku_joins.vendor_product_code
        ,sku_joins.is_bulk_receivable
        ,sku_joins.is_presellable
        ,sku_joins.is_virtual_inventory
        ,sku_joins.is_cargill
        ,sku_joins.is_edm
        ,sku_joins.is_marketplace
        ,sku_joins.is_rastellis
        ,coalesce(ais.is_always_in_stock,FALSE) as is_always_in_stock
        ,inventory_classification.inventory_classification as inventory_classification
        ,sku_joins.sku_vendor_name
        ,sku_joins.vendor_funded_discount_start_at_utc
        ,sku_joins.vendor_funded_discount_end_at_utc
        ,sku_joins.promotion_start_at_utc
        ,sku_joins.promotion_end_at_utc
        ,sku_joins.member_discount_start_at_utc
        ,sku_joins.member_discount_end_at_utc
        ,sku_joins.non_member_discount_start_at_utc
        ,sku_joins.non_member_discount_end_at_utc
        ,sku_joins.active_at_utc
        ,sku_joins.created_at_utc
        ,sku_joins.updated_at_utc
        ,sku_joins.dbt_valid_from
        ,sku_joins.dbt_valid_to
        ,sku_joins.adjusted_dbt_valid_from
        ,sku_joins.adjusted_dbt_valid_to
    from sku_joins
        left join ais on sku_joins.ais_id = ais.ais_id
        left join inventory_classification on sku_joins.inventory_classification_id = inventory_classification.inventory_classification_id
)

,add_standard_cost as (
    select
        add_inventory_classification.sku_id
        ,add_inventory_classification.sku_key
        ,add_inventory_classification.cut_id
        ,add_inventory_classification.cut_key
        ,add_inventory_classification.sku_vendor_id
        ,add_inventory_classification.sku_barcode
        ,add_inventory_classification.farm_id
        ,add_inventory_classification.farm_name
        ,add_inventory_classification.is_active_farm
        ,add_inventory_classification.category
        ,add_inventory_classification.sub_category
        ,add_inventory_classification.cut_name
        ,add_inventory_classification.sku_name
        ,add_inventory_classification.sku_weight
        ,add_inventory_classification.owned_sku_cost_usd
        ,add_inventory_classification.marketplace_cost_usd
        ,coalesce(standard_sku_cost.standard_sku_cost_usd,add_inventory_classification.owned_sku_cost_usd) as standard_sku_cost_usd
        ,add_inventory_classification.sku_cost_usd
        ,add_inventory_classification.platform_fee_usd
        ,add_inventory_classification.fulfillment_fee_usd
        ,add_inventory_classification.payment_processing_fee_usd
        ,add_inventory_classification.standard_price_usd
        ,add_inventory_classification.sku_price_usd
        ,add_inventory_classification.average_box_quantity
        ,add_inventory_classification.vendor_funded_discount_name
        ,add_inventory_classification.vendor_funded_discount_usd
        ,add_inventory_classification.vendor_funded_discount_percent
        ,add_inventory_classification.promotional_price_usd
        ,add_inventory_classification.member_discount_percent
        ,add_inventory_classification.non_member_discount_percent
        ,add_inventory_classification.replenishment_code
        ,add_inventory_classification.vendor_case_pack_quantity
        ,add_inventory_classification.vendor_product_code
        ,add_inventory_classification.is_bulk_receivable
        ,add_inventory_classification.is_presellable
        ,add_inventory_classification.is_virtual_inventory
        ,add_inventory_classification.is_cargill
        ,add_inventory_classification.is_edm
        ,add_inventory_classification.is_marketplace
        ,add_inventory_classification.is_rastellis
        ,add_inventory_classification.is_always_in_stock
        ,add_inventory_classification.inventory_classification
        ,add_inventory_classification.sku_vendor_name
        ,add_inventory_classification.vendor_funded_discount_start_at_utc
        ,add_inventory_classification.vendor_funded_discount_end_at_utc
        ,add_inventory_classification.promotion_start_at_utc
        ,add_inventory_classification.promotion_end_at_utc
        ,add_inventory_classification.member_discount_start_at_utc
        ,add_inventory_classification.member_discount_end_at_utc
        ,add_inventory_classification.non_member_discount_start_at_utc
        ,add_inventory_classification.non_member_discount_end_at_utc
        ,add_inventory_classification.active_at_utc
        ,add_inventory_classification.created_at_utc
        ,add_inventory_classification.updated_at_utc
        ,add_inventory_classification.dbt_valid_from
        ,add_inventory_classification.dbt_valid_to
        ,add_inventory_classification.adjusted_dbt_valid_from
        ,add_inventory_classification.adjusted_dbt_valid_to
        ,coalesce(cast(standard_sku_cost.standard_cost_added_date as date), cast(add_inventory_classification.dbt_valid_from as date)) as standard_cost_added_date
    from add_inventory_classification
        left Join standard_sku_cost on add_inventory_classification.sku_id = standard_sku_cost.sku_id
            and add_inventory_classification.dbt_valid_to is null
)

select * from add_standard_cost
