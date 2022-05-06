with

order_item as ( select * from {{ ref('order_item_details') }} )
,orders as (select order_id from {{ ref('stg_cc__orders')}} )
,coolant_cost as (select * from {{ ref('int_coolant_cost_per_order')}} )
,packaging_cost as (select * from {{ ref('int_packaging_cost_per_order')}} )
,care_cost as (select * from {{ ref('int_care_cost_per_order') }} )

,combined_costs as (
    select 
        orders.order_id
        ,order_coolant_cost
        ,order_packaging_cost
        ,order_care_cost
        ,sum(order_item.sku_cost) as product_cost
    from orders
        left join coolant_cost on orders.order_id = coolant_cost.order_id
        left join packaging_cost on orders.order_id = packaging_cost.order_id
        left join care_cost on orders.order_id = care_cost.order_id
        left join order_item on orders.order_id = order_item.order_id
    group by 1, 2, 3, 4
)


select * from combined_costs
