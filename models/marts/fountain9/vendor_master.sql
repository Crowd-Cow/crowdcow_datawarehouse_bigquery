with

receivable as ( select * from {{ ref('pipeline_receivables') }} )
,sku as ( select * from {{ ref('skus') }} )
,current_farm as ( select * from {{ ref('farms') }} where dbt_valid_to is null)
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )

,get_vendor_orders as (
    select
        receivable.pipeline_order_id
        ,receivable.farm_id
        ,current_farm.sku_vendor_id
        ,receivable.lot_number
        ,receivable.sku_id
        ,receivable.quantity_ordered
        ,receivable.farm_out_name
        ,sku_vendor.sku_vendor_name
        ,sku.sku_weight
        ,receivable.created_at_utc
    from receivable
        left join sku on receivable.sku_key = sku.sku_key
        left join current_farm on receivable.farm_id = current_farm.farm_id
        left join sku_vendor on current_farm.sku_vendor_id = sku_vendor.sku_vendor_id
    where not receivable.is_destroyed
        and not receivable.is_order_removed
        and not receivable.is_rastellis
        and receivable.created_at_utc >= dateadd('month',-6,sysdate())
        
)

,get_avg_sku_weight as (
    select
        sku_vendor_id
        ,sku_vendor_name
        ,avg(sku_weight) as average_sku_weight
    from get_vendor_orders
    group by 1,2
)

,calc_min_order_qty as (
    select
        sku_vendor_id as brand_id
        ,sku_vendor_name as brand
        ,floor(div0(1200,average_sku_weight)) as min_order_qty
        ,5000 as cost_to_ship
    from get_avg_sku_weight
    where sku_vendor_id is not null
)

select * from calc_min_order_qty
