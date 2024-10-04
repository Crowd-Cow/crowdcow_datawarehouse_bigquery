with

sub_promo as ( select * from {{ source('cc', 'subscription_promotions') }} )

,renamed as (
    select
        id as subscription_promotion_id
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
        ,subscription_id
        ,promotion_id
        ,promotion_selection_id
    from sub_promo
)

select * from renamed
