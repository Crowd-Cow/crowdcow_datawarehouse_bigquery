with source as (

  select * from  {{ ref('bid_items_ss') }} where not _fivetran_deleted

),

renamed as ( 

  select 
    created_at as created_at_utc
    ,{{ clean_strings('description') }} as bid_item_description
    ,{{ clean_strings('description_template') }} as bid_item_description_template
    ,event_id
    ,hide_from_user as is_hidden_from_user
    ,id as bid_item_id
    ,dbt_scd_id as bid_item_key
    ,{{ clean_strings('item_photo_url') }} as bid_item_photo_url
    ,{{ cents_to_usd('item_price_cents') }} as bid_item_price_usd
    ,{{ cents_to_usd('list_price_cents') }} as bid_item_list_price_usd
    ,{{ cents_to_usd('non_member_price_cents') }} as bid_item_non_member_price_usd
    ,{{ cents_to_usd('member_price_cents') }} as bid_item_member_price_usd
    ,{{ clean_strings('item_type') }} as bid_item_type
    ,{{ clean_strings('name') }} as bid_item_name
    ,{{ clean_strings('portion_type') }} as portion_type 
    ,quantity_available
    ,{{ cents_to_usd('strike_through_price_cents') }} as strike_through_price_usd
    ,{{ clean_strings('subtype') }} as bid_item_subtype
    ,{{ clean_strings('token') }} as bid_item_token
    ,updated_at as updated_at_utc
    ,product_variant_id
    ,max_dynamic_quantity  
    ,{{ clean_strings('highlight_text') }} as highlight_text
    ,stackable as is_stackable
    ,new_customers_only as is_new_customers_only
    ,bid_item_group_id
    ,limit_one_per_order as limit_one_per_order
    ,valid_price as is_valid_price
    ,{{ clean_strings('skus_with_quantities_hash') }} as skus_with_quantities_hash
    ,product_permutation_id
    ,automated_highlight_text_type
    ,always_available as is_always_available
    ,dbt_valid_to
    ,dbt_valid_from
    ,case
        when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
        else dbt_valid_from
      end as adjusted_dbt_valid_from
    , coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source 

)

select * from renamed
