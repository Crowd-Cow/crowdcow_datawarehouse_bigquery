with source as (

    select * from {{ ref('bid_item_sku_with_quantities_ss') }} where not _fivetran_deleted

),

renamed as (

    select
        id as bid_item_sku_quantity_id
        ,dbt_scd_id as bid_item_sku_quantity_key
        ,sku_id
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
        ,quantity as sku_quantity
        ,bid_item_id
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

