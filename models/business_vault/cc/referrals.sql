with

referrals as ( select * from {{ ref('stg_cc__referrals') }}  )

select * from referrals 
