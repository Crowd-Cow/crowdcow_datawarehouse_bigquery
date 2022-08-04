with

promo_code as ( select * from {{ ref('stg_cc__gift_codes') }} )
,partner as ( select * from {{ ref('stg_cc__partners') }} )

,dedup_redemptions as (
    select
        order_id
        ,partner_id
        ,created_at_utc
        ,redeemed_at_utc
    from promo_code
    where redeemed_at_utc is not null
        and order_id is not null
        and partner_id is not null
    qualify row_number() over(partition by order_id order by redeemed_at_utc desc) = 1
)

,get_partner_key as (
    select 
        dedup_redemptions.*
        ,partner.partner_key
    from dedup_redemptions
        left join partner on dedup_redemptions.partner_id = partner.partner_id
            and dedup_redemptions.created_at_utc >= partner.adjusted_dbt_valid_from
            and dedup_redemptions.created_at_utc < partner.adjusted_dbt_valid_to
)

select * from get_partner_key
