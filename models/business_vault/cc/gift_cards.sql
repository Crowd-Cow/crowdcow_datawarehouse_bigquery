with

gift_card as ( select * from {{ ref('stg_cc__gift_cards') }} )
,gift_info as ( select * from {{ ref('stg_cc__gift_infos') }} )

select
    gift_card.*
    ,gift_info.order_id
from gift_card
    left join gift_info on gift_card.gift_info_id = gift_info.gift_info_id
