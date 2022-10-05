with

gift_info as ( select * from {{ ref('stg_cc__gift_infos') }} )

select * from gift_info
