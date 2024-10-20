with

source as ( select * from {{ ref('product_quantity_availables_ss') }}  where (_fivetran_deleted is null or _fivetran_deleted = false)

)

,renamed as (
    select
        id as product_quantity_available_id
        ,fc_id
        ,quantity as quantity_available
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
        ,product_id
        ,dbt_scd_id as product_quantity_available_key
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
