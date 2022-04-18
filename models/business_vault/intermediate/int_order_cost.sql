with

order_item as ( select * from {{ ref('order_item_details') }} )

,order_cost_aggregation as (
    select
        order_id
        ,sum(sku_cost) as product_cost
    from order_item
    group by 1
)

select * from order_cost_aggregation
