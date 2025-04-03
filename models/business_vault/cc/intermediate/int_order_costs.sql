with

order_item as ( select * from {{ ref('order_item_details') }} )
,orders as (select order_id from {{ ref('stg_cc__orders')}} )
,coolant_cost as (select * from {{ ref('int_coolant_cost_per_order')}} )
,packaging_cost as (select * from {{ ref('int_packaging_cost_per_order')}} )
,care_cost as (select * from {{ ref('int_care_cost_per_order') }} )
,fc_labor_cost as ( select * from {{ ref('int_fc_labor_cost_per_order') }} )
,shipment as ( select * from {{ ref('shipments') }} )
,inbound_shipment as ( select * from {{ ref('int_inbound_shipping_costs') }} )

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

        ,coalesce(sum(sku_cost * sku_quantity),0) as product_cost
        ,coalesce(sum(total_sku_weight * inbound_shipment.lot_cost_per_pound),0) as inbound_shipping_cost
        
    from order_item
        left join inbound_shipment on order_item.lot_number = inbound_shipment.lot_number
    group by 1
)

,shipment_costs as (
    select
        order_id
        ,sum(shipment_postage_rate_usd) as shipment_cost
    from shipment
    group by 1
)

,combined_costs as (
    select 
        orders.order_id
        ,coalesce(order_coolant_cost, 0 ) as order_coolant_cost
        ,coalesce(order_packaging_cost, 0 ) as order_packaging_cost
        ,coalesce(order_care_cost, 0 ) as order_care_cost
        ,coalesce(item_detail_costs.product_cost, 0 ) as product_cost
        ,coalesce(item_detail_costs.poseidon_fulfillment_cost, 0 ) as poseidon_fulfillment_cost
        ,coalesce(item_detail_costs.inbound_shipping_cost, 0 ) as inbound_shipping_cost
        ,coalesce(fc_labor_cost.order_picking_cost, 0 ) as order_picking_cost
        ,coalesce(fc_labor_cost.order_packing_cost, 0 ) as order_packing_cost
        ,coalesce(fc_labor_cost.order_box_making_cost, 0 ) as order_box_making_cost
        ,coalesce(fc_labor_cost.order_fc_other_cost, 0 ) as order_fc_other_cost
        ,coalesce(fc_labor_cost.order_fc_labor_cost, 0 ) as order_fc_labor_cost
        ,coalesce(shipment_costs.shipment_cost, 0 ) as shipment_cost
    from orders
        left join item_detail_costs on orders.order_id = item_detail_costs.order_id
        left join coolant_cost on orders.order_id = coolant_cost.order_id
        left join packaging_cost on orders.order_id = packaging_cost.order_id
        left join care_cost on orders.order_id = care_cost.order_id
        left join fc_labor_cost on orders.order_id = fc_labor_cost.order_id
        left join shipment_costs on orders.order_id = shipment_costs.order_id
)


select * from combined_costs
