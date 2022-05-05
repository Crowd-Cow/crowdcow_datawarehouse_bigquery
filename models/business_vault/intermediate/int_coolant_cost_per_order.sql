with coolant_costs as (select * from {{ ref ( 'stg_gs__fc_care_packaging_costs' ) }} where cost_type = upper('coolant_cost'))
    ,orders as (select * from {{ ref('orders') }} )

,month_to_cost_timing as 
    (select month_of_costs
        ,fc_id
        ,cost_usd
        ,ifnull(lead(month_of_costs,1) over(partition by fc_id order by month_of_costs),'2999-01-01') as leading_month
    from coolant_costs
    where cost_type = upper('coolant_cost')
    order by fc_id, month_of_costs
     )
     

,coolant_cost_per_pound as (
    select date_trunc('month', orders.shipped_at_utc) as month_of_shipment
         ,orders.fc_id
         ,month_to_cost_timing.cost_usd
         ,sum(orders.coolant_weight_in_pounds) as total_coolant_pounds
         ,round(month_to_cost_timing.cost_usd/sum(orders.coolant_weight_in_pounds),2) as cost_per_pound_coolant
    from orders
        join month_to_cost_timing on date_trunc('month', orders.shipped_at_utc) < month_to_cost_timing.leading_month
                                  and orders.fc_id = month_to_cost_timing.fc_id
    group by 1, 2, 3
)


    select order_id
        ,orders.coolant_weight_in_pounds*coolant_cost_per_pound.cost_per_pound_coolant as order_coolant_cost
    from orders
        join coolant_cost_per_pound on date_trunc('month', orders.shipped_at_utc) = coolant_cost_per_pound.month_of_shipment
                                    and orders.fc_id = coolant_cost_per_pound.fc_id

