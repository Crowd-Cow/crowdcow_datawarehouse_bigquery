with stage as (

    select * from ref('stg_cc__orders')

)

select * from stage