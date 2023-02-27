with

order_item_detail as ( select * from {{ ref('order_item_details') }} where not is_rastellis or is_rastellis is null )
,sku as ( select * from {{ ref('skus') }} where dbt_valid_to is null and not is_rastellis )

select order_item_detail.*
    	,sku.sku_name
        ,sku.category
        ,sku.sub_category
        ,sku.farm_name
        ,sku.sku_weight
        ,sku.sku_price_usd
    from order_item_detail
    	join sku on order_item_detail.sku_id = sku.sku_id



