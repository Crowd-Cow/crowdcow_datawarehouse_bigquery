with

source as ( select * from {{ source('iterable', 'journeys') }} )

,renamed as (
    select
        id as workflow_id,
        {{ clean_strings('name') }} as workflow_name,
        enabled as is_enabled
    from source
)

select * from renamed
