with

pipeline_order as ( select * from {{ ref('stg_cc__pipeline_orders') }} )
,pipeline_receivable as ( select * from {{ ref('stg_cc__pipeline_receivables') }} )
,purchase_order as ( select * from {{ ref('stg_shipwell__purchase_orders') }} )
,purchase_order_line_item as ( select * from {{ ref('stg_shipwell__purchase_order_line_items') }} )
,shipment_financial as ( select * from {{ ref('stg_shipwell__shipment_financials') }} )

,total_ordered as (
    select
        pipeline_order.lot_number
        ,sum(pipeline_receivable.quantity_ordered) as quantity_ordered
    from pipeline_receivable
        left join pipeline_order on pipeline_receivable.pipeline_order_id = pipeline_order.pipeline_order_id
    where pipeline_receivable.marked_destroyed_at_utc is null
    group by 1
)

,total_lot_weight as (
    select
        lot_number
        ,shipment_id
        ,sum(item_total_weight) as total_lot_weight
    from purchase_order
        left join purchase_order_line_item on purchase_order.purchase_order_id = purchase_order_line_item.purchase_order_id
    group by 1,2
)

,get_shipment_id as (
    select
        total_ordered.*
        ,total_lot_weight.shipment_id
        ,total_lot_weight.total_lot_weight
    from total_ordered
        left join total_lot_weight on total_ordered.lot_number = total_lot_weight.lot_number
)

,get_shipment_amount as (
    select
        get_shipment_id.*
        ,shipment_financial.shipment_amount
    from get_shipment_id
        inner join shipment_financial on get_shipment_id.shipment_id = shipment_financial.shipment_id
)

,calc_per_unit_cost as (
    select
        *
        ,sum(total_lot_weight) over(partition by shipment_id) as total_shipment_weight
        ,div0(total_lot_weight,total_shipment_weight) as pct_lot_weight
        ,shipment_amount * pct_lot_weight as lot_cost
        ,div0(lot_cost,quantity_ordered) as lot_cost_per_unit
    from get_shipment_amount
)

select * from calc_per_unit_cost
