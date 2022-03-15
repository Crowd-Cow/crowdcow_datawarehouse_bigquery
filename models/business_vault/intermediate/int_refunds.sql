with

refund as ( select * from {{ ref('stg_stripe__refunds') }} )
,charge as ( select * from {{ ref('stg_stripe__charges') }} )

,refund_amounts as (
    select
        stripe_charge_id
        ,sum(refund_amount_usd) as refund_amount_usd
    from refund
    group by 1
)

,clean_partial_refunds as (
    select
        refund_amounts.stripe_charge_id
        ,case
            when charge.amount_refunded <> 0 and charge.charge_amount <> refund_amounts.refund_amount_usd then 0
            else refund_amounts.refund_amount_usd
        end as refund_amount_usd
    from refund_amounts
        left join charge on refund_amounts.stripe_charge_id = charge.stripe_charge_id
)

select * from clean_partial_refunds
