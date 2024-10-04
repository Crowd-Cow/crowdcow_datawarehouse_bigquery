with 
source as ( select * from {{ source('stripe', 'stripe_payment_methods') }} )
,card as ( select * from raw_stripe.stripe_payment_methods_card)
,wallet as ( select * from raw_stripe.stripe_payment_methods_card_wallet)

select
    source.id as payment_method_id
    ,{{ clean_strings('card.brand') }} as brand
    ,{{ clean_strings('card.funding') }} as funding_type
    ,{{ clean_strings('wallet.type') }} as wallet_type
from source
left join card on card.__stripe_payment_methods_panoply_id = source.__panoply_id
left join wallet on wallet.__stripe_payment_methods_card_panoply_id = card.__panoply_id