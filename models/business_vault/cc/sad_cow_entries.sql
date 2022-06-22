with

sad_cow as ( select * from {{ ref('stg_cc__sad_cow_entries') }} )
,lot as ( select * from {{ ref('stg_cc__lots') }} )
,sku_box as ( select * from {{ ref('stg_cc__sku_boxes') }} )
,sku as ( select * from {{ ref('stg_cc__skus') }} )

,get_keys as (
    select
        sad_cow.*
        ,lot.lot_key
        ,lot.fc_id
        ,sku_box.sku_box_key
        ,sku.sku_key
    from sad_cow
        left join lot on sad_cow.lot_id = lot.lot_id
            and sad_cow.created_at_utc >= lot.adjusted_dbt_valid_from
            and sad_cow.created_at_utc < lot.adjusted_dbt_valid_to
        left join sku_box on sad_cow.sku_box_id = sku_box.sku_box_id
            and sad_cow.created_at_utc >= sku_box.adjusted_dbt_valid_from
            and sad_cow.created_at_utc < sku_box.adjusted_dbt_valid_to
        left join sku on sad_cow.sku_id = sku.sku_id
            and sad_cow.created_at_utc >= sku.adjusted_dbt_valid_from
            and sad_cow.created_at_utc < sku.adjusted_dbt_valid_to
)

select * from get_keys
