with

    source as ( select * from {{ ref('stg_gs__product_master_v_2') }} )
 

select distinct
    cut_id
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
    ,active_solidus_inactive
    ,allocation_in_percentage
from source
    
