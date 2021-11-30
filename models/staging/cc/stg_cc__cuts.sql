with source as (

    select * from {{ source('cc', 'cuts') }} where not _fivetran_deleted

),

renamed as (

    select
        id as cut_id
        ,dbt_scd_id as cut_key
        ,sort_order
        ,product_subtype
        ,hard_scan_weight_min
        ,{{ clean_strings('pretty_name') }} as cut_pretty_name
        ,hard_scan_weight_max
        ,updated_at as updated_at_utc
        ,named_cut_weight_min
        ,plu
        ,primal_id
        ,named_cut_weight_max
        ,merchandising_target_weight
        ,average_box_quantity
        ,product_type
        ,created_at as created_at_utc
        ,sales_category
        ,{{clean_strings('name') }} as cut_name
        ,named_cut_weight_target
        ,rounding_rule
        ,{{ clean_strings('generic_description_override') }} as generic_description_override
        ,popular_cut as is_popular_cut
        ,in_use as is_in_use
        ,portion_cut as is_portion_cut
        ,yield_cut as is_yield_cut
        ,named_weight_cut as is_named_weight_cut
        ,sold_by_weight as is_sold_by_weight

    from source

)

select * from renamed

