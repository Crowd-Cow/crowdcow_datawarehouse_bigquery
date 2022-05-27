with

packing_action as (select * from {{ ref('stg_cc__packing_actions') }} )
,current_sku as (
        select 
            sku_id
            ,sku_name 
        from {{ ref('skus') }} 
        where dbt_valid_to is null
            and not (sku_weight <= 0.05 and category is null)
            and sku_name <> 'CROWD COW HANDWRITTEN NOTE'
)
,fc_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs') }} where cost_type = 'FC_LABOR_COST')
,order_info as (select * from {{ ref('stg_cc__orders') }} )

,pick_pack as (
    select
        packing_action.order_id
        ,packing_action.user_id
        ,packing_action.sku_id
        ,order_info.fc_id
        ,packing_action.input_method
        ,packing_action.action
        ,current_sku.sku_name
        ,packing_action.created_at_utc
        ,order_info.order_picked_at_utc
        from packing_action
            inner join order_info on packing_action.order_id = order_info.order_id
            inner join current_sku on packing_action.sku_id = current_sku.sku_id
)

,get_pick_pack_duration as (
    select
        user_id
        ,order_id
        ,action
        ,fc_id
        ,created_at_utc::date as dt
        ,date_trunc(month,order_picked_at_utc) as picking_month
        ,min(created_at_utc) as mn_dt
        ,max(created_at_utc) as mx_dt
        ,datediff(minute,min(created_at_utc),max(created_at_utc)) as minute_duration
        ,datediff(second,min(created_at_utc),max(created_at_utc)) as second_duration
    from pick_pack
    where action in ('ADD_TO_BOX','REMOVED_FROM_BOX','PACKED_ITEM')
    group by 1,2,3,4,5,6
)

,aggregate_order_labor_minutes as (
    select
        order_id
        ,picking_month
        ,sum(iff(action in ('ADD_TO_BOX','REMOVED_FROM_BOX'),minute_duration,0)) as picking_minute_duration
        ,sum(iff(action in ('PACKED_ITEM'),minute_duration,0)) as packing_minute_duration
    from get_pick_pack_duration
    group by 1,2
)

,calc_cost_per_labor_minutes as (
    select 
        month_of_costs
        ,ifnull(lead(month_of_costs) over(order by month_of_costs),'2999-01-01') as adjusted_month_of_costs
        ,cost_usd
        ,labor_hours
        ,(cost_usd/(labor_hours*60)) as cost_per_minute
    from fc_costs
)

,calc_per_order_cost as (
    select
        aggregate_order_labor_minutes.order_id
        ,aggregate_order_labor_minutes.picking_minute_duration * calc_cost_per_labor_minutes.cost_per_minute as picking_cost
        ,aggregate_order_labor_minutes.packing_minute_duration * calc_cost_per_labor_minutes.cost_per_minute as packing_cost
    from aggregate_order_labor_minutes
        left join calc_cost_per_labor_minutes on aggregate_order_labor_minutes.picking_month >= calc_cost_per_labor_minutes.month_of_costs
            and aggregate_order_labor_minutes.picking_month < calc_cost_per_labor_minutes.adjusted_month_of_costs
)

select * from calc_per_order_cost
