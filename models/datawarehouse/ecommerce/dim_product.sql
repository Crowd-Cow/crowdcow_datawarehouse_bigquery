with stage as (

    select * from {{ ref('stg_cc__products') }}

)

select * from stage
