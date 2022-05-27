with

order_item as ( select * from {{ ref('order_item_details') }} )
,orders as (select order_id from {{ ref('stg_cc__orders')}} )
,coolant_cost as (select * from {{ ref('int_coolant_cost_per_order')}} )
,packaging_cost as (select * from {{ ref('int_packaging_cost_per_order')}} )
,care_cost as (select * from {{ ref('int_care_cost_per_order') }} )
,pick_pack_cost as ( select * from {{ ref('int_pick_pack_cost_per_order') }} )

,item_detail_costs as (
    select
        order_id
        
        /** Poseidon only has two SKU items (A5 WAGYU STRIPLOIN STEAK TRIO, A5 WAGYU RIBEYE STEAK TRIO) that cost $50 per order. The rest are $40 per order. **/
        /** If there are ever additional SKU items, this may need to be updated. **/
        ,max(case
            when sku_id in (159054,137123,137122) and fc_id = 10 then 50
            when sku_id not in (159054,137123,137122) and fc_id = 10 then 40
            else 0
        end) as poseidon_fulfillment_cost

        ,sum(sku_cost * sku_quantity) as product_cost
        
    from order_item
    group by 1
)

,combined_costs as (
    select 
        orders.order_id
        ,order_coolant_cost
        ,order_packaging_cost
        ,order_care_cost
        ,item_detail_costs.product_cost
        ,item_detail_costs.poseidon_fulfillment_cost
        ,pick_pack_cost.picking_cost
        ,pick_pack_cost.packing_cost
    from orders
        left join item_detail_costs on orders.order_id = item_detail_costs.order_id
        left join coolant_cost on orders.order_id = coolant_cost.order_id
        left join packaging_cost on orders.order_id = packaging_cost.order_id
        left join care_cost on orders.order_id = care_cost.order_id
        left join pick_pack_cost on orders.order_id = pick_pack_cost.order_id
)


select * from combined_costs
