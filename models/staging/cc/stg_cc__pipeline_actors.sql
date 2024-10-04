with

source as ( select * from {{ source('cc', 'pipeline_actors') }} where not _fivetran_deleted )

,renamed as (
    select
        id as pipeline_actor_id
        ,updated_at as updated_at_utc
        ,{{ clean_strings('email_address') }} as email_address
        ,{{ clean_strings('actor_type') }} as actor_type
        ,erp_id
        ,{{ clean_strings('name') }} as pipeline_actor_name
        ,created_at as created_at_utc
        ,actor_id
        ,notify_of_payment as should_notify_of_payment
    from source
)

select * from renamed
