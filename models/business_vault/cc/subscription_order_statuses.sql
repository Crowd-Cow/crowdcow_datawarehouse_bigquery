with

subscription_statuses as ( select * from {{ ref('stg_cc__subscription_statuses') }} )

select *
from subscription_statuses