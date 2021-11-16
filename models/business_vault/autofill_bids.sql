with

bid_log as ( select * from {{ ref('stg_cc__autofill_bid_logs') }} )
,autofill_order as ( select * from {{ ref('stg_cc__autofill_order_logs') }} )
,sku as ( select * from {{ ref('skus') }} )

,autofill_joins as (
    select
        bid_log.autofill_bid_log_id
        ,bid_log.target_sku_id
        ,bid_log.target_product_permutation_id
        ,bid_log.autofill_sku_id
        ,bid_log.autofill_product_permutation_id
        ,autofill_order.order_id
        ,autofill_order.previous_order_id
        ,bid_log.bid_id
        ,{{ get_join_key('skus','sku_key','sku_id','bid_log','autofill_sku_id','created_at_utc') }} as autofill_sku_key
        ,{{ get_join_key('skus','sku_key','sku_id','bid_log','target_sku_id','created_at_utc') }} as target_sku_key
        ,bid_log.target_sku_name
        ,bid_log.target_quantity
        ,bid_log.autofill_sku_name
        ,bid_log.autofill_quantity
        ,autofill_order.autofill_type
        ,bid_log.reason
        ,autofill_order.notes
        ,autofill_order.filled_at_date
        ,bid_log.created_at_utc
        ,bid_log.updated_at_utc

    from bid_log
        left join autofill_order on bid_log.autofill_order_log_id = autofill_order.autofill_order_log_id
)

select * from autofill_joins
    