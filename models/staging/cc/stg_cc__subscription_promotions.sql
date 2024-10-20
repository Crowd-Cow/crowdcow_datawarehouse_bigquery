with

sub_promo as ( select * from {{ source('cc', 'subscription_promotions') }} where __deleted is null )
,new_sub_promo as ( select * from {{ source('cc', 'promotions_subscription_enrollments') }} where __deleted is null)

,renamed as (
    select
        id as subscription_promotion_id
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
        ,subscription_id
        ,promotion_id
        ,promotion_selection_id
    from sub_promo
    union all
    select 
        id as subscription_promotion_id
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
        ,subscription_id
        ,promotions_promotion_id as promotion_id
        ,null as promotion_selection_id
    from new_sub_promo
)

select * from renamed
