with source as (

    select * from {{ ref('ambassadors_ss') }} where not _fivetran_deleted

),

renamed as (

  select
    id              as ambassador_id
    ,dbt_scd_id as ambassador_key
    ,user_id
    ,partner_id
    ,sort_order     as ambassador_sort_order -- Appears to be for use in the app
    ,{{ clean_strings('data_fields') }}     as ambassador_data_fields -- Complex string
    ,{{ clean_strings('google_fields') }}   as ambassador_google_fields -- Complex string
    ,{{ clean_strings('category') }}        as ambassador_category
    ,{{ clean_strings('status') }}          as ambassador_status
    ,{{ clean_strings('email') }}           as ambassador_email
    ,created_at     as created_at_utc
    ,updated_at     as updated_at_utc
    ,introduced_at  as ambassador_introduced_at_utc
    ,profile_image_height   as ambassador_profile_image_height
    ,profile_image_width    as ambassador_profile_image_width
    ,lifestyle_image_height as ambassador_lifestyle_image_height
    ,lifestyle_image_width  as ambassador_lifestyle_image_width
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
