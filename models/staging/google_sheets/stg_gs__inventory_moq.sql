with

source as ( select * from {{ source('google_sheets', 'inventory_moq') }} )

,renamed as (
    select
        {{ dbt_utils.surrogate_key(['farm_id','cut_id']) }} as moq_id
        ,farm_id
        ,{{ clean_strings('farm_name') }} as farm_name
        ,cast(plu as string) as plu
        ,cut_id
        ,{{ clean_strings('cut_name') }} as cut_name
        ,plu_weight
        ,batch_size
        ,case_weight
        ,cast(if(is_primary_vendor = 'yes',true,false) as boolean) as is_primary_vendor
        ,cast(if(is_secondary_vendor = 'yes',true,false) as boolean) as is_secondary_vendor
        ,moq
    from source
)

select * from renamed
