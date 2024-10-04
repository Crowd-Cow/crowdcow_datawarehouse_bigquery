with 

tracking_detail as ( select * from raw_mysql.tracking_details )

,renamed as (

  select 
    id as tracking_detail_id
    ,shipment_id
    ,country
    ,upper(trim( message )) as message
    ,upper(trim( source )) as carrier
    ,raw_json as full_json
    ,upper(trim( exception_type )) as extension_type
    ,upper(trim( city )) as city
    ,upper(trim( state )) as state
    ,zip
    ,upper(trim( status )) as status
    ,created_at as created_at_utc
    ,tracking_updated_at as tracking_updated_at_utc
    ,updated_at as updated_at_utc

  from tracking_detail
)

select * from renamed
