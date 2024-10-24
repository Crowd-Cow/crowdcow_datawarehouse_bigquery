with source as (

    select * from {{ ref('sku_reservations_ss') }}  where __deleted is null 

),

renamed as (

    select
        id as sku_reservation_id
        ,dbt_scd_id as sku_reservation_key
        --,reservation_group_id
        ,quantity as sku_reservation_quantity
        ,{{ cents_to_usd('price_in_cents') }} as price_usd
        ,{{ cents_to_usd('cost_in_cents') }} as cost_usd
        ,bid_id
        ,manually_changed_at as manually_changed_at_utc
        ,{{ cents_to_usd('fulfillment_fee_in_cents') }} as fulfillment_fee_usd
        ,bid_item_id
        ,order_id
        ,pick_list_id
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,{{ cents_to_usd('payment_processing_fee_in_cents') }} as payment_processing_fee_usd
        ,{{ cents_to_usd('platform_fee_in_cents') }} as platform_fee_usd
        ,sku_id
        ,original_quantity
        ,fc_id
        --,sku_reservation_pool_id
        ,dbt_valid_to
        ,dbt_valid_from
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
         end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed

