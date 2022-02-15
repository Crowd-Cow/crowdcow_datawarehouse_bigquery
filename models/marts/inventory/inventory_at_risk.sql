with

inventory as ( select * from {{ ref('inventory_snapshot') }} )
,forecast as ( select * from {{ ref('stg_forecasting__cat_subcat_daily') }} )
,sku as ( select * from {{ ref('skus') }} )
,fc as ( select * from {{ ref('fcs') }} )
,receivable as ( select * from {{ ref('pipeline_receivables') }} )

,inventory_aggregation as (
    select
        inventory.snapshot_date
        ,inventory.fc_id
        ,fc.fc_name
        ,sku.category
        ,sku.sub_category
        ,sku.sku_id
        ,sku.cut_id
        ,sku.cut_name
        ,{{ dbt_utils.surrogate_key(['inventory.snapshot_date','sku.category','sku.sub_category','sku.cut_id','inventory.fc_id']) }} as join_key
        ,sum(inventory.quantity) as quantity
        ,sum(inventory.potential_revenue) as potential_revenue
        ,sum(inventory.quantity_reserved) as quantity_reserved
        ,sum(inventory.quantity_sellable) as quantity_sellable
    from inventory
        left join sku on inventory.sku_key = sku.sku_key
        left join fc on inventory.fc_key = fc.fc_key
    where not inventory.is_destroyed
    group by 1,2,3,4,5,6,7,8
)

,inventory_forecast as (
    select
        forecast_date
        ,category
        ,sub_category
        ,cut_id
        ,fc_id
        ,{{ dbt_utils.surrogate_key(['forecast_date','category','sub_category','cut_id','fc_id']) }} as join_key
        ,p50 as forecasted_sales
        ,sum(case when p50 < 0 then 0 else p50 end) 
            over(partition by category,sub_category,cut_id,fc_id 
                order by forecast_date rows between unbounded preceding and unbounded following)/12 as avg_forecasted_weekly_units
    from forecast
)

,first_available_pipeline_order as (
    select
        sku_id
        ,fc_id
        ,fc_scan_proposed_date::date as fc_scan_proposed_date
        ,sum(quantity) as ordered_quantity
    from receivable
    where not is_destroyed
    group by 1,2,3
)

,join_forecast as (
    select distinct
        inventory_aggregation.sku_id
        ,inventory_aggregation.fc_id
        ,inventory_aggregation.snapshot_date
        ,inventory_aggregation.fc_name
        ,inventory_aggregation.category
        ,inventory_aggregation.sub_category
        ,inventory_aggregation.cut_name
        ,inventory_aggregation.quantity
        ,inventory_aggregation.quantity_reserved
        ,inventory_aggregation.quantity_sellable
        ,inventory_aggregation.potential_revenue
        ,round(inventory_forecast.avg_forecasted_weekly_units,2) as avg_forecasted_weekly_units
        
        ,first_value(first_available_pipeline_order.fc_scan_proposed_date) 
            over(partition by inventory_aggregation.sku_id,inventory_aggregation.fc_id,inventory_aggregation.snapshot_date 
                order by first_available_pipeline_order.fc_scan_proposed_date) as next_pipeline_order_date
                
        ,first_value(first_available_pipeline_order.ordered_quantity) 
            over(partition by inventory_aggregation.sku_id,inventory_aggregation.fc_id,inventory_aggregation.snapshot_date 
                order by first_available_pipeline_order.fc_scan_proposed_date) as next_order_quantity

    from inventory_aggregation
        inner join inventory_forecast on inventory_aggregation.join_key = inventory_forecast.join_key
        left join first_available_pipeline_order on inventory_aggregation.sku_id = first_available_pipeline_order.sku_id
            and inventory_aggregation.fc_id = first_available_pipeline_order.fc_id
            and inventory_aggregation.snapshot_date <= first_available_pipeline_order.fc_scan_proposed_date
)

,calcs as (
    select *
    ,div0(potential_revenue,quantity) * avg_forecasted_weekly_units as avg_weekly_potential_revenue
    ,div0(quantity_sellable,avg_forecasted_weekly_units) as wos
    ,div0(quantity,avg_forecasted_weekly_units) as est_wos_total
    ,div0(quantity_reserved,quantity_sellable) as pct_reserved
    ,sysdate()::date 
        + (div0(quantity_sellable,avg_forecasted_weekly_units) 
        * 7 
        * (1 - div0(quantity_reserved,quantity_sellable)))::int as est_oos_date
    from join_forecast
)

select * from calcs
