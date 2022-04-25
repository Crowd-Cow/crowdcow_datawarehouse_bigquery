with

sub_promo as ( select * from {{ ref('stg_cc__subscription_promotions') }} )
,promo as ( select * from {{ ref('stg_cc__promotions') }} )

,dedup_promos as (
    select
        *
        ,iff(promotion_id = 35,promotion_selection_id,promotion_id) as selected_promotion_id
    from sub_promo
    qualify row_number() over(partition by subscription_id order by updated_at_utc desc) = 1
)

,get_promo_type as (
    select
        dedup_promos.*
        ,promo.promotion_type
    from dedup_promos
        left join promo on dedup_promos.selected_promotion_id = promo.promotion_id
            and dedup_promos.created_at_utc >= promo.adjusted_dbt_valid_from
            and dedup_promos.created_at_utc < promo.adjusted_dbt_valid_to
)

select * from get_promo_type
