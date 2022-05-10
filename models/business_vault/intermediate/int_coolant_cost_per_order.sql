with 

coolant_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs')}} where cost_type = 'COOLANT_COST')
,orders as (select * from {{ ref('stg_cc__orders') }})
,shipments as (select order_id, max(shipped_at_utc) as shipped_at_utc from {{ ref('stg_cc__shipments') }} group by 1)

,order_details as (
    select 
        orders.*
        ,shipments.shipped_at_utc
    from orders
    left join shipments on orders.order_id = shipments.order_id
)

,month_to_cost_timing as (
    select 
        month_of_costs
        ,fc_id
        ,cost_usd
        ,ifnull(lead(month_of_costs,1) over(partition by fc_id order by month_of_costs),'2999-01-01') as leading_month
    from coolant_costs
)

,coolant_cost_per_pound as (
    select 
        date_trunc('month', order_details.shipped_at_utc) as month_of_shipment
         ,order_details.fc_id
         ,month_to_cost_timing.cost_usd
         ,sum(order_details.coolant_weight_in_pounds) as total_coolant_pounds
         ,round(month_to_cost_timing.cost_usd/sum(order_details.coolant_weight_in_pounds),2) as cost_per_pound_coolant
    from order_details
        join month_to_cost_timing on date_trunc('month', order_details.shipped_at_utc) >= month_to_cost_timing.month_of_costs
            and date_trunc('month', order_details.shipped_at_utc) < month_to_cost_timing.leading_month
            and order_details.fc_id = month_to_cost_timing.fc_id
    group by 1, 2, 3
)

select 
    order_details.order_id
    ,order_details.coolant_weight_in_pounds*coolant_cost_per_pound.cost_per_pound_coolant as order_coolant_cost
from order_details
    join coolant_cost_per_pound on date_trunc('month', order_details.shipped_at_utc) = coolant_cost_per_pound.month_of_shipment
        and order_details.fc_id = coolant_cost_per_pound.fc_id
