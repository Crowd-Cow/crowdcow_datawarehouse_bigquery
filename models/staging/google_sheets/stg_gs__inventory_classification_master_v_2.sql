with

source as ( select * from {{ source('google_sheets', 'inventory_classification_master_v_2') }} )

,renamed as (
    select
        {{ dbt_utils.surrogate_key(['category','sub_category','cut_name']) }} as inventory_classification_id
        ,{{ clean_strings('category') }} as category
        ,{{ clean_strings('sub_category') }} as sub_category
        ,{{ clean_strings('cut_name') }} as cut_name
        ,{{ clean_strings('combo') }} as combo
        ,{{ clean_strings('inventory_classification') }} as inventory_classification
    from source
)

select * from renamed
