with source as (

    select * from {{ source('cc', 'handwritten_notes') }} where not _fivetran_deleted

),

renamed as (

    select
        id as note_id
        ,{{ clean_strings('the_text') }} as the_text
        ,{{ clean_strings('image_url') }} as image_url
        ,order_id
        ,backdrop_style_name
        ,created_at as created_at_utc
        ,key as key
        ,handwriting_style
    from source

)
select * from renamed

