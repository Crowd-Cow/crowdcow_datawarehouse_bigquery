with

source as ( select * from {{ source('shipwell', 'shipwell_shipmenttags') }} )

,renamed as (
    select
        id as shipment_tag_id
        ,{{ clean_strings('color') }} as tag_color
        ,{{ clean_strings('name') }} as tag_name
        ,company as company_id
    from source
)

select * from renamed
