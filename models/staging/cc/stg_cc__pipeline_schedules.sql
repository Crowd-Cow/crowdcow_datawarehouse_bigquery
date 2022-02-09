with source as (

    select * from {{ ref('pipeline_schedules_ss') }} where not _fivetran_deleted

),

renamed as (

    select
        id as pipeline_schedule_id
        , dbt_scd_id as pipeline_schedule_key
        , {{ clean_strings('status') }} as status
        , updated_at as updated_at_utc
        , fc_id
        , created_by
        , {{ clean_strings('schedule_type') }} as schedule_type
        , created_at as created_at_utc
        , actual_date 
        , farm_id
        , quantity
        , offsite_storage_id
        , pipeline_order_id
        , pipeline_actor_id
        , proposed_date
        , butcher_id
        , dbt_valid_from
        , dbt_valid_to
        , case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        , coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed

