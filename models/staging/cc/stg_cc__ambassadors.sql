{{
  config(
    tags=["stage"]
  )
}}

with source as (

    select * from {{ source('cc', 'ambassadors') }}

),

renamed as (

    select
         id             as ambassador_id
        ,data_fields    as ambassador_data_fields
        ,user_id
        ,partner_id
        ,google_fields  as ambassador_google_fields
        ,category       as ambassador_category
        ,status         as ambassador_status
        ,sort_order
        ,email          as ambassador_email
        ,created_at     as created_at_utc
        ,updated_at     as updated_at_utc
        ,introduced_at  as ambassador_introduced_at_utc
        ,profile_image_height   as ambassador_profile_image_height
        ,profile_image_width    as ambassador_profile_image_width
        ,lifestyle_image_height as ambassador_lifestyle_image_height
        ,lifestyle_image_width  as ambassador_lifestyle_image_width

    from source

)

select * from renamed
