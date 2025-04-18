
with

inventory as ( 
    select 
        date(snapshot_date) as snapshot_date
        ,fc_id
        ,quantity
        ,potential_revenue
        ,quantity_reserved
        ,quantity_sellable
        ,sku_cost
        ,sku_key
        ,fc_key
        ,is_destroyed
    from {{ ref('inventory_snapshot') }} )
,forecast as ( select * from {{ ref('demand_forecast') }} )
,sku as ( select * from {{ ref('skus') }} )
,fc as ( select * from {{ ref('fcs') }} )
,receivable as ( select * from {{ ref('pipeline_receivables') }} )

,inventory_aggregation_sku as (
    select
        inventory.snapshot_date 
        ,inventory.fc_id
        ,fc.fc_name
        ,sku.category
        ,sku.sub_category
        ,sku.sku_id
        ,sku.cut_id
        ,sku.cut_name
        ,sku.inventory_classification
        ,coalesce(sku.is_always_in_stock,FALSE) as is_always_in_stock
        ,{{ dbt_utils.surrogate_key(['date(inventory.snapshot_date)','sku.category','sku.sub_category','sku.cut_id','inventory.fc_id']) }} as join_key
        ,sum(inventory.quantity) as quantity
        ,sum(inventory.potential_revenue) as potential_revenue
        ,max(inventory.quantity_reserved) as quantity_reserved
        ,sum(inventory.quantity_sellable) as quantity_sellable
        ,sum(inventory.sku_cost) as sku_cost
    from inventory
        left join sku on inventory.sku_key = sku.sku_key
        left join fc on inventory.fc_key = fc.fc_key
    where not inventory.is_destroyed
    group by inventory.snapshot_date,2,3,4,5,6,7,8,9,10
)

,first_available_pipeline_order as (
    select
        sku_id
        ,fc_id
        ,lot_number
        ,farm_out_name as farm_name
        ,cast(fc_scan_proposed_date as date) as fc_scan_proposed_date
        ,sum(quantity_ordered) as quantity_ordered
    from receivable
    where not is_destroyed
    group by 1,2,3,4,5
)

,add_next_order as (
    select distinct
        inventory_aggregation_sku.snapshot_date
        ,inventory_aggregation_sku.fc_id
        ,inventory_aggregation_sku.fc_name
        ,inventory_aggregation_sku.category
        ,inventory_aggregation_sku.sub_category
        ,inventory_aggregation_sku.sku_id
        ,inventory_aggregation_sku.cut_id
        ,inventory_aggregation_sku.cut_name
        ,inventory_aggregation_sku.inventory_classification
        ,inventory_aggregation_sku.is_always_in_stock
        ,inventory_aggregation_sku.join_key
        ,inventory_aggregation_sku.quantity
        ,inventory_aggregation_sku.potential_revenue
        ,inventory_aggregation_sku.quantity_reserved
        ,inventory_aggregation_sku.quantity_sellable
        ,inventory_aggregation_sku.sku_cost
    
        ,first_value(first_available_pipeline_order.fc_scan_proposed_date) 
            over(partition by inventory_aggregation_sku.category,inventory_aggregation_sku.sub_category,inventory_aggregation_sku.cut_id,inventory_aggregation_sku.fc_id,inventory_aggregation_sku.snapshot_date 
                order by first_available_pipeline_order.fc_scan_proposed_date) as next_pipeline_order_date

        ,first_value(first_available_pipeline_order.quantity_ordered) 
            over(partition by inventory_aggregation_sku.category,inventory_aggregation_sku.sub_category,inventory_aggregation_sku.cut_id,inventory_aggregation_sku.fc_id,inventory_aggregation_sku.snapshot_date 
                order by first_available_pipeline_order.fc_scan_proposed_date) as next_order_quantity
    
        ,first_value(first_available_pipeline_order.farm_name) 
            over(partition by inventory_aggregation_sku.category,inventory_aggregation_sku.sub_category,inventory_aggregation_sku.cut_id,inventory_aggregation_sku.fc_id,inventory_aggregation_sku.snapshot_date 
                order by first_available_pipeline_order.fc_scan_proposed_date) as next_order_farm
    
        ,first_value(first_available_pipeline_order.lot_number) 
            over(partition by inventory_aggregation_sku.category,inventory_aggregation_sku.sub_category,inventory_aggregation_sku.cut_id,inventory_aggregation_sku.fc_id,inventory_aggregation_sku.snapshot_date 
                order by first_available_pipeline_order.fc_scan_proposed_date) as next_order_lot_number
    
    from inventory_aggregation_sku
        left join first_available_pipeline_order on inventory_aggregation_sku.sku_id = first_available_pipeline_order.sku_id
            and inventory_aggregation_sku.fc_id = first_available_pipeline_order.fc_id
            and inventory_aggregation_sku.snapshot_date <= first_available_pipeline_order.fc_scan_proposed_date
        
)

,inventory_aggregation_cut as (
    select
        snapshot_date
        ,fc_name
        ,category
        ,sub_category
        ,cut_name
        ,inventory_classification
        ,is_always_in_stock
        ,join_key
        ,next_pipeline_order_date
        ,next_order_quantity
        ,next_order_farm
        ,next_order_lot_number
        ,sum(quantity) as quantity
        ,sum(potential_revenue) as potential_revenue
        ,max(quantity_reserved) as quantity_reserved
        ,sum(quantity_sellable) as quantity_sellable
        ,sum(sku_cost) as sku_cost
    from add_next_order 
    group by 1,2,3,4,5,6,7,8,9,10,11,12
)

,inventory_forecast as (
    select
        forecast_date
        ,category
        ,sub_category
        ,cut_id
        ,fc_id
        ,{{ dbt_utils.surrogate_key(['date(forecast_date)','category','sub_category','cut_id','fc_id']) }} as join_key
        ,forecasted_sales
        ,avg(forecasted_sales) 
            over(
                partition by category,sub_category,cut_id,fc_id 
                order by forecast_date 
                rows between current row and 6 following
            ) as next_seven_day_avg
    from forecast
)

,join_forecast as (
    select distinct
        inventory_aggregation_cut.snapshot_date
        ,inventory_aggregation_cut.fc_name
        ,inventory_aggregation_cut.category
        ,inventory_aggregation_cut.sub_category
        ,inventory_aggregation_cut.cut_name
        ,inventory_aggregation_cut.inventory_classification
        ,inventory_aggregation_cut.is_always_in_stock
        ,inventory_aggregation_cut.next_pipeline_order_date
        ,inventory_aggregation_cut.next_order_quantity
        ,inventory_aggregation_cut.next_order_farm
        ,inventory_aggregation_cut.next_order_lot_number
        ,inventory_aggregation_cut.quantity
        ,inventory_aggregation_cut.quantity_reserved
        ,inventory_aggregation_cut.quantity_sellable
        ,inventory_aggregation_cut.potential_revenue
        ,inventory_aggregation_cut.sku_cost
        ,round(inventory_forecast.forecasted_sales,2) as daily_forecasted_sales
        ,round(inventory_forecast.next_seven_day_avg*7,2) as avg_forecasted_weekly_units

    from inventory_aggregation_cut
        left join inventory_forecast on inventory_aggregation_cut.join_key = inventory_forecast.join_key
)

,calcs as (
    select *
    ,safe_divide(potential_revenue,quantity) * avg_forecasted_weekly_units as avg_weekly_potential_revenue
    ,if(safe_divide(potential_revenue,quantity) * avg_forecasted_weekly_units > potential_revenue,safe_divide(potential_revenue,quantity) * avg_forecasted_weekly_units - potential_revenue,0) as potential_missed_revenue
    ,safe_divide(quantity_sellable,avg_forecasted_weekly_units) as wos
    ,safe_divide(quantity,avg_forecasted_weekly_units) as est_wos_total
    ,safe_divide(quantity_reserved,quantity_sellable) as pct_reserved
    ,DATE_ADD(
        CURRENT_DATE(),
        INTERVAL CAST(
            SAFE_DIVIDE(quantity_sellable, avg_forecasted_weekly_units) * 7 * (1 - SAFE_DIVIDE(quantity_reserved, quantity_sellable))
            AS INT64
        ) DAY
        ) AS est_oos_date
    from join_forecast
)

,add_risk_flags as (
    select
        {{ dbt_utils.surrogate_key(['snapshot_date', 'fc_name','category','sub_category','cut_name','is_always_in_stock']) }} as snapshot_id
        ,*
        ,is_always_in_stock and wos < 1 as is_oos_sku
        ,is_always_in_stock and wos between 1 and 4 and est_oos_date < next_pipeline_order_date as is_at_risk_sku
        ,is_always_in_stock and est_wos_total > wos and wos < 2 as should_check_with_fc
    from calcs
)

,add_ranks as (
    select
        *
        ,rank() over(partition by snapshot_date,is_oos_sku order by avg_weekly_potential_revenue desc) as is_oos_sku_rank
        ,rank() over(partition by snapshot_date,is_at_risk_sku order by avg_weekly_potential_revenue desc) as is_at_risk_rank
        ,rank() over(partition by snapshot_date,should_check_with_fc order by avg_weekly_potential_revenue desc) as should_check_with_fc_rank
    from add_risk_flags
)

select * from add_ranks
