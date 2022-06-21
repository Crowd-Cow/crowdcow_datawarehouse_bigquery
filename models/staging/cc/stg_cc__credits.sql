with source as (

    select * from {{ source('cc', 'credits') }} where not _fivetran_deleted

),

renamed as (

    select
        id as credit_id
        ,promotion_id
        ,bid_item_id
        ,cow_cash_entry_source_id
        ,iff(promotion_id is not null and promotion_type is null,'PROMOTION',{{ clean_strings('promotion_type') }} ) as promotion_source -- default to old promotion table source if type is null
        ,{{ cents_to_usd('discount_in_cents') }} as credit_discount_usd
        ,created_at as created_at_utc
        ,user_id
        ,{{ clean_strings('credit_type') }} as credit_type
        ,order_id
        ,bid_id
        ,updated_at as updated_at_utc
        ,{{ clean_strings('description') }} as credit_description
        ,{{ convert_percent('discount_percent') }} as discount_percent
        ,hide_from_user as is_hidden_from_user
        ,controlled_by_promotion as is_controlled_by_promotion
    from source

)

select * 
from renamed 

/**** Filter out records that come from the new promotions table, that have a promotion_id and the credit amount is 0 ****/
/**** This was a system bug that was fixed on 2022-06-15. This should only filter out a few hundred invalid credit records ****/
where not (
    promotion_source = 'PROMOTIONS::PROMOTION' 
    and promotion_id is not null
    and credit_discount_usd = 0 
)
