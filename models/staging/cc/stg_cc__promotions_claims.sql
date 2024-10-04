with

source as ( select * from {{ source('cc', 'promotions_claims') }} where not _fivetran_deleted )

,renamed as (
    select
        id as promotion_claims_id
        ,user_id
        ,promotions_promotion_id
        ,order_id
        ,{{ clean_strings('promo_code') }} as promo_code
        ,claimed_at as claimed_at_utc
        ,unclaimed_at as unclaimed_at_utc
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
    from source
)

select * from renamed
