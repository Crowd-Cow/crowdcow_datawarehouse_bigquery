with

tag as ( select * from {{ ref('stg_cc__tags') }} )

select
    *
from tag
