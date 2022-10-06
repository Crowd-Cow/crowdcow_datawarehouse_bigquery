with

purchase_order as ( select * from {{ ref('pipeline_order_receiving') }} where not is_rastellis or is_rastellis is null )
,sku as ( select * from {{ ref('skus') }})
,current_fc as ( select * from {{ ref('fcs') }} where dbt_valid_to is null )
,pipeline_order as ( select * from {{ ref('stg_cc__pipeline_orders') }} )
,pipeline_schedule as ( select * from {{ ref('pipeline_schedules') }} )

,join_data as (
    select
        sku.cut_id
        ,sku.cut_name
        ,sku.category
        ,sku.sub_category
        ,purchase_order.fc_id
        ,current_fc.fc_name
        ,purchase_order.pipeline_order_id
        ,purchase_order.lot_number
        ,sku.sku_vendor_id as supplier_id
        ,sku.sku_vendor_name as supplier_name
        ,pipeline_order.created_at_utc as po_date
        ,pipeline_schedule.fc_in_actual_date as received_date
        ,sum(purchase_order.quantity_ordered) as quantity_ordered
        ,sum(purchase_order.total_sku_cost_ordered) as value_ordered
        ,sum(purchase_order.quantity_received) as quantity_received
        ,sum(purchase_order.total_sku_cost_received) as value_received
    from purchase_order
        left join sku on purchase_order.sku_key = sku.sku_key
        left join current_fc on purchase_order.fc_id = current_fc.fc_id
        left join pipeline_order on purchase_order.pipeline_order_id = pipeline_order.pipeline_order_id
        left join pipeline_schedule on purchase_order.pipeline_order_id = pipeline_schedule.pipeline_order_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12
)

select * 
from join_data 
where fc_id not in (8,10,14) --filter out Poseidon, Nationwide, and Valmeyer FCs
    and po_date >= '2021-01-01'
