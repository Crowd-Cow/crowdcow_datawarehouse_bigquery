with

source as ( select * from {{ ref('offsite_storages_ss') }}  )

,renamed as (
    select
        id as offsite_storage_id
        ,created_at as created_at_utc
        ,{{ clean_strings('name') }} as offsite_storage_name
        ,updated_at as updated_at_utc
        ,in_service as is_in_service
        ,dbt_scd_id as offsite_storage_key
        ,dbt_updated_at
        ,dbt_valid_from
        ,dbt_valid_to

        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
         end as adjusted_dbt_valid_from

        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to
    from source
)

select * from renamed
