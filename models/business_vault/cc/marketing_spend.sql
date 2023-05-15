with

marketing_budget as (select * from {{ ref('stg_gs__marketing_spend') }})


select *
from marketing_budget