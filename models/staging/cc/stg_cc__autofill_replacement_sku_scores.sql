with

source as (select * from {{ ref('autofill_replacement_sku_scores_ss') }} where __deleted is null and (_fivetran_deleted is null or _fivetran_deleted = false) )

,renamed as (
    select
        updated_at as updated_at_utc
        ,score as replacement_score
        ,replacement_sku_id
        ,sku_id
        ,created_at as created_at_utc
        ,id as autofilL_replacement_score_id
        ,dbt_scd_id as autofill_replacement_score_key
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
