with source as (

    select * from {{ source('cc', 'box_types') }} 

),

renamed as (
    select id as box_id
           ,fc_id
           ,height_in_inches
           ,width_in_inches
           ,{{ clean_strings('name')  }} as box_name
    from source
)

select * 
from renamed
