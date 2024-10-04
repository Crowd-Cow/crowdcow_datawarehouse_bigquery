with

source as ( select * from {{ source('reference_data', 'axlehire_postal_code_markets') }} )

,renamed as (
    select distinct
        lpad(postal_code,5,'00000') as postal_code
        ,zone as axlehire_zone
        ,market as axlehire_market
        ,iff(center_zone = '√',TRUE,FALSE) as is_center_zone
        ,sub_region
    from source
)

select * from renamed
