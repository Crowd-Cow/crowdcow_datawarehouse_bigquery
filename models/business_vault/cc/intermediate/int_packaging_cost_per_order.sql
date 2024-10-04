with 

packaging_costs as (select * from {{ ref('stg_gs__fc_care_packaging_costs') }} where cost_type = 'BOX_COST')
,box_types as (select * from  {{ ref( 'stg_cc__box_types' )}} )
,shipment_details as (select * from {{ ref( 'stg_cc__shipments') }} where shipped_at_utc is not null)

,month_to_cost_timing as (
    select month_of_costs
        ,fc_id
        ,cost_usd
        ,box_type
        ,ifnull(lead(month_of_costs,1) over(partition by fc_id, box_type order by month_of_costs),'2999-01-01') as leading_month
    from packaging_costs
)

,get_box_id as (
    select 
        month_to_cost_timing.month_of_costs
        ,month_to_cost_timing.fc_id
        ,month_to_cost_timing.cost_usd
        ,month_to_cost_timing.box_type
        ,month_to_cost_timing.leading_month
        ,box_types.box_id
    from month_to_cost_timing
        inner join box_types on month_to_cost_timing.box_type = box_types.box_name
            and month_to_cost_timing.fc_id = box_types.fc_id
    )


,total_packaging_per_order as (
    select 
        shipment_details.order_id
        ,sum(get_box_id.cost_usd) as order_packaging_cost
    from shipment_details
        join get_box_id on coalesce(shipment_details.scanned_box_type_id,shipment_details.box_type_id) = get_box_id.box_id
            and date_trunc(shipment_details.shipped_at_utc, MONTH) >= get_box_id.month_of_costs
            and date_trunc(shipment_details.shipped_at_utc, MONTH) < get_box_id.leading_month
    group by 1
)
    
select *
from total_packaging_per_order
