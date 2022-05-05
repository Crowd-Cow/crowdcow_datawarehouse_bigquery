with care_costs as (select * from {{ ref ( 'stg_gs__fc_care_packaging_costs' ) }} where cost_type = 'CS_LABOR_COST')
    ,orders as (select * from {{ ref('orders') }} )

,month_to_cost_timing as 
    (select month_of_costs
        ,cost_usd
        , cost_type
        , ifnull(lead(month_of_costs,1) over(order by month_of_costs),'2999-01-01') as leading_month
    from care_costs
    order by month_of_costs
     )
     
 ,avg_care_costs_per_order as (
    select date_trunc('month', orders.shipped_at_utc) as month_of_shipment
         ,count(distinct order_id) 
            as total_orders
         ,month_to_cost_timing.cost_usd as monthly_care_cost
         ,round(month_to_cost_timing.cost_usd/count(distinct orders.order_id),2) as care_cost_per_order
    from orders
        join month_to_cost_timing on orders.shipped_at_utc >= month_to_cost_timing.month_of_costs
                                  and orders.shipped_at_utc < month_to_cost_timing.leading_month
    group by 1, 3
)

,care_cost_per_order as (
    select orders.order_id
        ,avg_care_costs_per_order.care_cost_per_order as order_care_cost
    from orders
        left join avg_care_costs_per_order on date_trunc('month', orders.shipped_at_utc) = avg_care_costs_per_order.month_of_shipment
)

select *
from care_cost_per_order