with

    source as ( select * from {{ source('google_sheets', 'product_master_v_2') }} )
 

select 
    {{ dbt_utils.surrogate_key(['farm_id','cut_id']) }} as moq_id
    ,cut_id
    ,cut_name
    ,ifnull(category,'NONE') as category
    ,ifnull(sub_category,'NONE') as sub_category
    ,combo
    ,farm_id
    ,brand
    ,plu
    ,plu_weight
    ,batch_size
    ,case_weight
    ,is_primary_vendor
    ,active_inactive
    ,allocation_in_percentage
from source
    
