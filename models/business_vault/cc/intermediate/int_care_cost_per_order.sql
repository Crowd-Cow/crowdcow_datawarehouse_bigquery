with 

care_costs as (select * from {{ ref ( 'stg_gs__fc_care_packaging_costs' ) }} where cost_type = 'CS_LABOR_COST')
,shipments as (select order_id, max(shipped_at_utc) as shipped_at_utc from {{ ref('stg_cc__shipments') }} where shipped_at_utc is not null group by 1)

,get_monthly_shipments as (
    select
        date_trunc(month,shipped_at_utc) as shipped_month
        ,count(distinct order_id) as order_count
    from shipments
    where shipped_at_utc is not null
    group by 1
)

,get_monthly_care_costs as (
    select
        get_monthly_shipments.*
        ,care_costs.cost_usd
        ,div0(care_costs.cost_usd,get_monthly_shipments.order_count) as care_cost_per_order
        ,ifnull(lead(month_of_costs,1) over(order by month_of_costs),'2999-01-01') as adjusted_date
    from get_monthly_shipments
        inner join care_costs on get_monthly_shipments.shipped_month = care_costs.month_of_costs
)

select
    shipments.order_id
    ,round(get_monthly_care_costs.care_cost_per_order,2) as order_care_cost
        
from shipments
    left join get_monthly_care_costs on shipments.shipped_at_utc >= get_monthly_care_costs.shipped_month
        and shipments.shipped_at_utc < get_monthly_care_costs.adjusted_date
