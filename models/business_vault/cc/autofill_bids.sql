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
        ,autofill_sku.sku_key as autofill_sku_key
        ,target_sku.sku_key as target_sku_key
        ,bid_log.target_sku_name
        ,bid_log.target_quantity
        ,bid_log.target_quantity * target_sku.sku_price_usd as target_sku_gross_product_revenue
        ,bid_log.autofill_sku_name
        ,bid_log.autofill_quantity
        ,bid_log.autofill_quantity * autofill_sku.sku_price_usd as autofill_sku_gross_product_revenue
        ,autofill_order.autofill_type
        ,bid_log.reason
        ,autofill_order.notes
        ,autofill_order.filled_at_date
        ,bid_log.created_at_utc
        ,bid_log.updated_at_utc

    from bid_log
        left join autofill_order on bid_log.autofill_order_log_id = autofill_order.autofill_order_log_id
        left join sku as autofill_sku on bid_log.autofill_sku_id = autofill_sku.sku_id
            and bid_log.created_at_utc >= autofill_sku.adjusted_dbt_valid_from
            and bid_log.created_at_utc < autofill_sku.adjusted_dbt_valid_to
        left join sku as target_sku on bid_log.target_sku_id = target_sku.sku_id
            and bid_log.created_at_utc >= target_sku.adjusted_dbt_valid_from
            and bid_log.created_at_utc < target_sku.adjusted_dbt_valid_to
)

,get_final_sku_autofill_action as (
    select
        *

        ,last_value(autofill_type || '|' || reason)  --concatenating two fields to make the "iff" logic easier
            over(partition by order_id,target_sku_id 
                order by created_at_utc,autofill_bid_log_id) as last_autofill_reason

        ,last_value(autofill_bid_log_id) 
            over(partition by order_id,target_sku_id 
                order by created_at_utc,autofill_bid_log_id) as last_autofill_bid_log_id

        ,iff(
            autofill_bid_log_id = last_autofill_bid_log_id
                and last_autofill_reason in ('REMOVAL|REMOVAL')
            ,autofill_quantity
            ,0
        ) as net_autofill_quantity
        
        ,iff(
            autofill_bid_log_id = last_autofill_bid_log_id
                and last_autofill_reason in ('REMOVAL|REMOVAL')
            ,autofill_sku_gross_product_revenue
            ,0
        ) as net_autofill_gross_product_revenue

    from autofill_joins
)

select * from get_final_sku_autofill_action
    