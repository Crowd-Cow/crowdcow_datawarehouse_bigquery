with source as (

  select * from {{ source('cc', 'packing_actions') }} where not _fivetran_deleted

),

renamed as (
    select id as packing_action_id
        ,sku_box_id
        ,user_id
        ,sku_id
        ,fc_location_id
        ,shipment_id
        ,barcode
        ,order_id
        ,{{ clean_strings( 'input_method' ) }} as input_method
        ,{{ clean_strings( 'action' ) }} as action
        ,{{ clean_strings( 'name') }} as details
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
    from source
)

select *
from renamed