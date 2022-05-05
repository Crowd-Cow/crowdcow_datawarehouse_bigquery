with

order_item as ( select * from {{ ref('order_item_details') }} )
,orders as (select order_id from {{ ref('stg_cc__orders')}} )
,coolant_cost as (select * from {{ ref('int_coolant_cost_per_order')}} )
,packaging_cost as (select * from {{ ref('int_packaging_cost_per_order')}} )
,care_cost as (select * from {{ ref('int_care_cost_per_order') }} )

,order_cost_aggregation as (
    select
        order_id
        ,sum(sku_cost) as product_cost
    from order_item
    group by 1
)

,static_gross_margin_costs as (
    select 
        orders.order_id
        ,order_coolant_cost
        ,order_packaging_cost
        ,order_care_cost
    from orders
        left join coolant_cost on orders.order_id = coolant_cost.order_id
        left join packaging_cost on orders.order_id = packaging_cost.order_id
        left join care_cost on orders.order_id = care_cost.order_id
)

,combined_costs as (
    select static_gross_margin_costs.order_id
        ,static_gross_margin_costs.order_coolant_cost
        ,static_gross_margin_costs.order_packaging_cost
        ,static_gross_margin_costs.order_care_cost
        ,order_cost_aggregation.product_cost
    from static_gross_margin_costs
        left join order_cost_aggregation on static_gross_margin_costs.order_id = order_cost_aggregation.order_id
)

select * from combined_costs
