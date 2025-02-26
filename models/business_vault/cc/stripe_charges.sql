with

stripe_charges as ( select * from {{ ref('stg_stripe__charges') }} )

select *
from stripe_charges