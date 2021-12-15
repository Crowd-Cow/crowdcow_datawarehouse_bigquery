with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_item as ( select * from {{ ref('order_items') }} )
,credit as ( select * from {{ ref('stg_cc__credits') }} )
,refund as ( select * from {{ ref('stg_cc__refunds') }} )

,bid_amounts as (
    select 
        order_id
        ,sum(order_item_revenue) as product_revenue_usd
        ,sum(order_item_discount) as order_item_discount_usd
    from order_item
    group by 1
)

,credit_amounts as (
    select
        order_id
        ,sum(discount_percent) as discount_percent
        ,sum(credit_discount_usd) as discount_amount_usd
    from credit
    group by 1
)

,refund_amounts as (
    select
        order_id
        ,sum(refund_amount_usd) as refund_amount_usd
    from refund
    group by 1
)

,revenue_joins as (
    select
        orders.order_id
        ,orders.order_shipping_fee_usd
        ,zeroifnull(bid_amounts.product_revenue_usd) as product_revenue_usd
        ,zeroifnull(bid_amounts.order_item_discount_usd) as order_item_discount_usd
        ,zeroifnull(credit_amounts.discount_amount_usd) as discount_amount_usd
        ,zeroifnull(refund_amounts.refund_amount_usd) as refund_amount_usd
        ,zeroifnull(credit_amounts.discount_percent) as discount_percent
    from orders
        left join bid_amounts on orders.order_id = bid_amounts.order_id
        left join credit_amounts on orders.order_id = credit_amounts.order_id
        left join refund_amounts on orders.order_id = refund_amounts.order_id
)

,revenue_calculations as (
    select
        order_id
        ,order_shipping_fee_usd
        ,product_revenue_usd + order_shipping_fee_usd as gross_revenue_usd
        ,product_revenue_usd
        ,discount_amount_usd + order_item_discount_usd as discount_amount_usd
        ,discount_percent
        ,refund_amount_usd
    from revenue_joins
)

select * from revenue_calculations
