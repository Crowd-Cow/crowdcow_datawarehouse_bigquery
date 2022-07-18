with

source as ( select * from {{ source('reference_data', 'ip_lookup') }} ) 

,renamed as (
    select
        query as ip_address
        ,zip as postal_code
        ,country
        ,city
        ,org
        ,hosting as is_server
        ,timezone
        ,isp
        ,region_name
        ,mobile as is_mobile
        ,lon as longitude
        ,lat as latitude
        ,"as" as autonomous_system
        ,country_code
        ,region
        ,status
        ,message
    from source
)

select * from renamed
