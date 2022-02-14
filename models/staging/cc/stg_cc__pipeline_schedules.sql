with source as (

    select * from {{ source('cc', 'pipeline_schedules') }} where not _fivetran_deleted

),

renamed as (

    select
        id as pipeline_schedule_id
        ,{{ clean_strings('status') }} as status
        ,updated_at as updated_at_utc
        ,fc_id
        ,created_by
        ,{{ clean_strings('schedule_type') }} as schedule_type
        ,created_at as created_at_utc
        ,actual_date 
        ,farm_id
        ,quantity
        ,offsite_storage_id
        ,pipeline_order_id
        ,pipeline_actor_id
        ,proposed_date
        ,butcher_id

    from source

)

select * from renamed

