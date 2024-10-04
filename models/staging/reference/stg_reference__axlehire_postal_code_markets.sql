with

source as ( select * from {{ source('reference_data', 'axlehire_postal_code_markets') }} )

,renamed as (
    select distinct
        lpad(CAST(postal_code AS STRING),5,'0') as postal_code
        ,zone as axlehire_zone
        ,market as axlehire_market
        ,if(center_zone = '√',TRUE,FALSE) as is_center_zone
        ,sub_region
    from source
)

select * from renamed
