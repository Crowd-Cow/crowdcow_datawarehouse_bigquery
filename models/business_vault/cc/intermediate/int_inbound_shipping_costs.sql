with

purchase_order as ( select * from {{ ref('stg_shipwell__purchase_orders') }} )
,purchase_order_line_item as ( select * from {{ ref('stg_shipwell__purchase_order_line_items') }} )
,shipment_financial as ( select * from {{ ref('stg_shipwell__shipment_financials') }} )

,total_lot_weight as (
    select
        lot_number
        ,shipment_id
        ,sum(item_total_weight) as total_lot_weight
    from purchase_order
        left join purchase_order_line_item on purchase_order.purchase_order_id = purchase_order_line_item.purchase_order_id
    group by 1,2
)

,get_shipment_amount as (
    select
        total_lot_weight.*
        ,shipment_financial.shipment_amount
    from total_lot_weight
        inner join shipment_financial on total_lot_weight.shipment_id = shipment_financial.shipment_id
)

,calc_per_unit_cost as (
    select
        *
        ,sum(total_lot_weight) over(partition by shipment_id) as total_shipment_weight
        ,div0(shipment_amount,total_shipment_weight) as lot_cost_per_pound
    from get_shipment_amount
)

select * from calc_per_unit_cost
