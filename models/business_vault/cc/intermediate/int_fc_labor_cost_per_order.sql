with

order_info as (select * from {{ ref('stg_cc__orders') }} )
,shipments as ( select order_id,count(*) as order_shipment_count from {{ ref('stg_cc__shipments') }} group by 1)
,pick_pack_duration as ( select * from {{ ref('int_pick_pack_durations') }} )
,fiscal_calendar as (select * from {{ ref('retail_calendar') }} )
,fc_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs') }} where cost_type = 'FC_LABOR_COST')
,fc_labor_hours as ( select * from {{ ref('fc_labor_hours') }} )

,orders_packed as (
    select 
        order_info.order_id
        ,date(fiscal_year,fiscal_month,1) as packing_fiscal_month
        ,shipments.order_shipment_count
        ,count(distinct order_info.order_id) as packed_order_count
        ,sum(if(pick_pack_duration.action = 'PACKED_ITEM',item_count,0)) as packed_item_count
        ,sum(if(pick_pack_duration.action in ('ADD_TO_BOX','REMOVED_FROM_BOX'),hour_duration,0)) as picking_hours
    from order_info
        left join pick_pack_duration on order_info.order_id = pick_pack_duration.order_id
        left join shipments on order_info.order_id = shipments.order_id
        left join fiscal_calendar on cast(order_info.order_packed_at_utc as date) = fiscal_calendar.calendar_date
    where order_info.order_packed_at_utc is not null
        and order_info.fc_id <> 10 -- FC labor costs should not be applied to Poseidon drop ship orders
    group by 1,2,3
)

,aggregate_pick_pack as (
    select
        packing_fiscal_month
        ,sum(picking_hours) as picking_hours
        ,sum(packed_order_count) as packed_order_count
        ,sum(order_shipment_count) as order_shipment_count
        ,sum(packed_item_count) as packed_item_count
    from orders_packed
    group by 1
)

,adjust_fc_cost_month as (
    select
        cost_usd
        ,month_of_costs
        ,packed_order_count
        ,order_shipment_count
        ,packed_item_count
        ,coalesce(lead(month_of_costs) over(order by month_of_costs),'2999-01-01') as adjusted_month
    from fc_costs
        left join aggregate_pick_pack on cast(fc_costs.month_of_costs as date) = aggregate_pick_pack.packing_fiscal_month
)

,fc_hours_by_category as (
    select distinct
        labor_fiscal_month
        ,hours_category
        ,sum(cast(total_hours_worked as FLOAT64) + cast(pto_hours as FLOAT64)) as total_hours
    from fc_labor_hours
    where fc_name is not null
    group by 1,2
)

,fc_adjusted_month as (
    select
        labor_fiscal_month
        
        ,if(
            max(labor_fiscal_month) over(order by total_hours) = labor_fiscal_month
            ,'2999-01-01'
            ,date_add(labor_fiscal_month, INTERVAL 1 MONTH)
        ) as adjusted_labor_fiscal_month
        ,hours_category
        ,total_hours
    from fc_hours_by_category
)

,get_monthly_cost as (
    select
        adjust_fc_cost_month.month_of_costs
        ,adjust_fc_cost_month.adjusted_month
        ,fc_adjusted_month.labor_fiscal_month
        ,fc_adjusted_month.adjusted_labor_fiscal_month
        ,adjust_fc_cost_month.cost_usd
        ,adjust_fc_cost_month.packed_order_count
        ,adjust_fc_cost_month.order_shipment_count
        ,adjust_fc_cost_month.packed_item_count
        
        ,fc_adjusted_month.hours_category
        ,fc_adjusted_month.total_hours
    from adjust_fc_cost_month
        left join fc_adjusted_month on cast(adjust_fc_cost_month.month_of_costs as date) >= fc_adjusted_month.labor_fiscal_month
            and cast(adjust_fc_cost_month.month_of_costs as date) < fc_adjusted_month.adjusted_labor_fiscal_month
)

,get_monthly_cost_components as (
    select
        aggregate_pick_pack.packing_fiscal_month
        ,get_monthly_cost.packed_order_count
        ,get_monthly_cost.order_shipment_count
        ,get_monthly_cost.packed_item_count
        ,get_monthly_cost.hours_category
        ,if(get_monthly_cost.hours_category = 'PICKING',aggregate_pick_pack.picking_hours,get_monthly_cost.total_hours) as total_hours
        ,get_monthly_cost.cost_usd
    from aggregate_pick_pack
        left join get_monthly_cost on cast(aggregate_pick_pack.packing_fiscal_month as date) >= cast(get_monthly_cost.month_of_costs as date)
            and cast(aggregate_pick_pack.packing_fiscal_month as date) < cast(get_monthly_cost.adjusted_month as date)
)

,calc_per_hour_cost as (
    select
        *
        ,sum(total_hours) over(partition by packing_fiscal_month) as monthly_work_hours
        ,SAFE_DIVIDE(cost_usd, sum(total_hours) over(partition by packing_fiscal_month)) as per_hour_cost
        ,SAFE_DIVIDE(cost_usd, sum(total_hours) over(partition by packing_fiscal_month)) * total_hours as category_cost
    from get_monthly_cost_components
)

,calc_per_order_costs as (
    select
        *
        ,case
            when hours_category = 'PACKING' then SAFE_DIVIDE(category_cost,order_shipment_count)
            when hours_category = 'PICKING' then SAFE_DIVIDE(category_cost,packed_item_count)
            when hours_category = 'BOX MAKING' then SAFE_DIVIDE(category_cost,order_shipment_count)
            when hours_category = 'ALL OTHER' then SAFE_DIVIDE(category_cost,packed_item_count)
        end as per_order_cost
    from calc_per_hour_cost
)

,get_per_order_costs as (
    select
        orders_packed.order_id
        ,orders_packed.packing_fiscal_month
        ,orders_packed.packed_order_count
        ,orders_packed.order_shipment_count
        ,orders_packed.packed_item_count
        ,calc_per_order_costs.hours_category
        ,calc_per_order_costs.per_order_cost
    from orders_packed
        left join calc_per_order_costs on orders_packed.packing_fiscal_month = calc_per_order_costs.packing_fiscal_month
)

,fc_cost_breakout as (
    select
        order_id
        ,packing_fiscal_month
        ,packed_item_count
        ,order_shipment_count
        ,sum(if(hours_category = 'PICKING',per_order_cost * packed_item_count,0)) as order_picking_cost
        ,sum(if(hours_category = 'PACKING',per_order_cost * order_shipment_count,0)) as order_packing_cost
        ,sum(if(hours_category = 'BOX MAKING',per_order_cost * order_shipment_count,0)) as order_box_making_cost
        ,sum(if(hours_category = 'ALL OTHER',per_order_cost * packed_item_count,0)) as order_fc_other_cost
    from get_per_order_costs
    group by 1,2,3,4
)

select
    order_id
    ,packing_fiscal_month
    ,order_picking_cost
    ,order_packing_cost
    ,order_box_making_cost
    ,order_fc_other_cost

    ,order_picking_cost
    + order_packing_cost
    + order_box_making_cost
    + order_fc_other_cost as order_fc_labor_cost
from fc_cost_breakout
