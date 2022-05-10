with 

coolant_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs')}} where cost_type = 'COOLANT_COST')
,orders as (select * from {{ ref('stg_cc__orders') }})
,shipments as (select order_id, max(shipped_at_utc) as shipped_at_utc from {{ ref('stg_cc__shipments') }} where shipped_at_utc is not null group by 1)

,get_coolant_used as (
    select
        orders.order_id
        ,orders.fc_id
        ,orders.coolant_weight_in_pounds
        ,shipments.shipped_at_utc
        ,date_trunc(month,shipments.shipped_at_utc) as shipped_month
    from orders
        left join shipments on orders.order_id = shipments.order_id
)

,get_monthly_coolant_usage as (
    select
        shipped_month
        ,fc_id
        ,sum(coolant_weight_in_pounds) as total_monthly_coolant_pounds
        ,count(order_id) as order_count
    from get_coolant_used
    group by 1,2
)

,calc_cost_per_pound as (
    select
        get_monthly_coolant_usage.*
        ,coolant_costs.cost_usd
        ,ifnull(lead(coolant_costs.month_of_costs,1) over(partition by coolant_costs.fc_id order by coolant_costs.month_of_costs),'2999-01-01') as adjusted_date
        ,div0(cost_usd,total_monthly_coolant_pounds) as coolant_cost_per_pound
    from get_monthly_coolant_usage
        inner join coolant_costs on get_monthly_coolant_usage.shipped_month = coolant_costs.month_of_costs
            and get_monthly_coolant_usage.fc_id = coolant_costs.fc_id
)

select
    get_coolant_used.order_id
    
    ,round(
        get_coolant_used.coolant_weight_in_pounds*calc_cost_per_pound.coolant_cost_per_pound
    ,2) as order_coolant_cost

from get_coolant_used
    left join calc_cost_per_pound on get_coolant_used.shipped_month >= calc_cost_per_pound.shipped_month
        and get_coolant_used.shipped_month < calc_cost_per_pound.adjusted_date
        and get_coolant_used.fc_id = calc_cost_per_pound.fc_id
