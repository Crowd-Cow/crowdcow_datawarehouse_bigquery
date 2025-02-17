with

fc as ( select * from {{ ref('stg_cc__fcs') }} )

select
    *
    ,case
        when fc_id in (4,5,17,18) then 'CROWD COW FC'
        when fc_id = 10 then 'DROP SHIP'
        else 'OTHER'
    end as fc_type
from fc
