with source as (

    select * from {{ ref('cuts_ss') }} where not _fivetran_deleted

),

renamed as (

    select
        id as cut_id
        ,dbt_scd_id as cut_key
        ,sort_order
        ,{{ clean_strings('product_subtype') }} as product_subtype
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
        ,{{ clean_strings('product_type') }} as product_type
        ,created_at as created_at_utc
        ,{{ clean_strings('sales_category') }} as sales_category
        ,{{ clean_strings('name') }} as cut_name
        ,named_cut_weight_target
        ,{{ clean_strings('rounding_rule') }} as rounding_rule
        ,{{ clean_strings('generic_description_override') }} as generic_description_override
        ,popular_cut as is_popular_cut
        ,in_use as is_in_use
        ,portion_cut as is_portion_cut
        ,yield_cut as is_yield_cut
        ,named_weight_cut as is_named_weight_cut
        ,sold_by_weight as is_sold_by_weight
        ,dbt_valid_to
        ,dbt_valid_from
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed
