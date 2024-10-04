with source as (

    select * from {{ source('cc', 'farm_vendor_tags') }} where not _fivetran_deleted

),

renamed as (

    select
        id as farm_vendor_tag_id
        ,updated_at as updated_at_utc
        ,farm_id
        ,created_at as created_at_utc
        ,vendor_tag_id

    from source

)

select * from renamed

