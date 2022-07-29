with

ordered_item as ( select * from {{ ref('pipeline_receivables') }} where not is_destroyed )
,received_item as ( select * from {{ ref('sku_lots') }} )
,current_sku as ( select * from {{ ref('skus') }} where dbt_valid_to is null)
,current_lot as ( select * from {{ ref('lots') }} where dbt_valid_to is null)
,vendor as ( select * from {{ ref('stg_cc__sku_vendors') }})
,pipeline_schedule as ( select * from {{ ref('pipeline_schedules') }} )
,invoice as ( select distinct invoice_key,bill_amount,bill_description from {{ ref('acumatica_invoices') }} )
,invoice_lot as ( select distinct invoice_key,bill_amount,bill_description from {{ ref('acumatica_invoices') }} where regexp_like(bill_description,'[0-9]{4}') )
,approved_invoice as ( select * from {{ ref('stg_gs__approved_invoice_lot_mapping') }} )

,get_ordered_detail as (
    select
        ordered_item.lot_number
        ,ordered_item.sku_id
        ,current_sku.sku_key
        ,ordered_item.fc_id
        ,ordered_item.pipeline_order_id
        ,current_lot.delivered_at_utc
        ,ordered_item.processor_out_name as processor_name

        ,case
            when vendor.is_marketplace and ordered_item.cost_per_unit_usd is null and current_sku.marketplace_cost_usd > 0 then current_sku.marketplace_cost_usd
            when (not vendor.is_marketplace or (vendor.is_marketplace and current_sku.marketplace_cost_usd = 0)) and ordered_item.cost_per_unit_usd is null then current_sku.owned_sku_cost_usd
            else ordered_item.cost_per_unit_usd
         end as cost_per_unit_usd

        ,vendor.is_marketplace
        ,sum(nullif(current_sku.sku_weight,0) * ordered_item.quantity_ordered) as sku_weight_ordered
        ,sum(ordered_item.quantity_ordered) as quantity_ordered
    from ordered_item
        left join current_lot on ordered_item.lot_number = current_lot.lot_number
        left join current_sku on ordered_item.sku_id = current_sku.sku_id
        left join vendor on current_lot.owner_id = vendor.sku_vendor_id
    group by 1,2,3,4,5,6,7,8,9
)

,get_received_detail as (
    select
        current_lot.lot_number
        ,received_item.sku_id
        ,current_sku.sku_key
        ,received_item.sku_lot_quantity as quantity_received
        ,current_lot.pipeline_order_id
        ,current_lot.fc_id
        ,current_lot.delivered_at_utc
        ,pipeline_schedule.processor_out_name as processor_name
        ,nullif(current_sku.sku_weight,0) * received_item.sku_lot_quantity as sku_weight_received
        ,iff(vendor.is_marketplace and current_sku.marketplace_cost_usd > 0, current_sku.marketplace_cost_usd,current_sku.owned_sku_cost_usd) as finance_cost
        ,vendor.is_marketplace
    from received_item
        left join current_lot on received_item.lot_id = current_lot.lot_id
        left join pipeline_schedule on current_lot.lot_number = pipeline_schedule.lot_number
        left join current_sku on received_item.sku_id = current_sku.sku_id
        left join vendor on current_lot.owner_id = vendor.sku_vendor_id
)

,get_ordered_received as (
    select
        coalesce(get_ordered_detail.lot_number,get_received_detail.lot_number) as lot_number
        ,coalesce(get_ordered_detail.pipeline_order_id,get_received_detail.pipeline_order_id) as pipeline_order_id
        ,coalesce(get_ordered_detail.sku_id,get_received_detail.sku_id) as sku_id
        ,coalesce(get_ordered_detail.sku_key,get_received_detail.sku_key) as sku_key
        ,coalesce(get_ordered_detail.fc_id,get_received_detail.fc_id) as fc_id
        ,coalesce(get_ordered_detail.processor_name,get_received_detail.processor_name) as processor_name
        ,coalesce(get_ordered_detail.cost_per_unit_usd,get_received_detail.finance_cost) as cost_per_unit_usd
        ,coalesce(get_ordered_detail.is_marketplace,get_received_detail.is_marketplace) as is_marketplace
        ,zeroifnull(get_ordered_detail.quantity_ordered) as quantity_ordered
        ,zeroifnull(get_ordered_detail.sku_weight_ordered) as sku_weight_ordered
        ,zeroifnull(get_received_detail.quantity_received) as quantity_received
        ,zeroifnull(get_received_detail.sku_weight_received) as sku_weight_received
        ,coalesce(get_ordered_detail.delivered_at_utc,get_received_detail.delivered_at_utc) as delivered_at_utc
    from get_ordered_detail
        full outer join get_received_detail on get_ordered_detail.lot_number = get_received_detail.lot_number
            and get_ordered_detail.sku_id = get_received_detail.sku_id
)

,calc_sku_lot_costs as (
    select
        *
        ,quantity_ordered * cost_per_unit_usd as total_sku_cost_ordered
        ,quantity_received * cost_per_unit_usd as total_sku_cost_received
        ,sum(quantity_ordered * cost_per_unit_usd) over(partition by lot_number) as total_lot_cost_ordered
        ,sum(quantity_received * cost_per_unit_usd) over(partition by lot_number) as total_lot_cost_received
    from get_ordered_received
)

,approved_invoice_details as (
    select
        coalesce(approved_invoice.lot_number,invoice_lot.bill_description) as lot_number
        ,sum(invoice.bill_amount) as total_invoice_usd
    from invoice
        left join approved_invoice on invoice.invoice_key = approved_invoice.invoice_key
        left join invoice_lot on invoice.invoice_key = invoice_lot.invoice_key
    group by 1       
)

,get_invoice_details as (
    select
        calc_sku_lot_costs.*
        ,zeroifnull(approved_invoice_details.total_invoice_usd) as total_invoice_usd
    from calc_sku_lot_costs
        left join approved_invoice_details on calc_sku_lot_costs.lot_number = approved_invoice_details.lot_number
)

,final_calcs as (
    select
        *
        ,div0(total_sku_cost_received,total_lot_cost_received) as pct_of_cost_received
        ,round(
            div0(total_sku_cost_received,total_lot_cost_received) * total_invoice_usd
        ,2) as total_sku_cost_invoiced
    from get_invoice_details
)

select distinct
    {{ dbt_utils.surrogate_key(['lot_number','sku_id']) }} as order_received_id
    ,lot_number
    ,sku_id
    ,sku_key
    ,fc_id
    ,pipeline_order_id
    ,processor_name
    ,cost_per_unit_usd
    ,quantity_ordered
    ,sku_weight_ordered
    ,total_sku_cost_ordered
    ,total_lot_cost_ordered
    ,quantity_received
    ,sku_weight_received
    ,total_sku_cost_received
    ,total_lot_cost_received
    ,total_sku_cost_invoiced
    ,total_invoice_usd
    ,pct_of_cost_received
    ,is_marketplace
    ,delivered_at_utc
from final_calcs
where lot_number <> '0000'
