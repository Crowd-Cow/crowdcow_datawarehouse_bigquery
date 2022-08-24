with

product_quantity as ( select * from {{ ref('stg_cc__product_quantity_availables') }} )
,current_fc as ( select * from {{ ref('stg_cc__fcs') }} where dbt_valid_to is null )
,current_product as ( select * from {{ ref('stg_cc__products') }} where dbt_valid_to is null )

,get_keys as (
    select
        product_quantity.*
        ,current_fc.fc_key
        ,current_product.product_key
    from product_quantity
        left join current_fc on product_quantity.fc_id = current_fc.fc_id
        left join current_product on product_quantity.product_id = current_product.product_id
)

select * from get_keys
