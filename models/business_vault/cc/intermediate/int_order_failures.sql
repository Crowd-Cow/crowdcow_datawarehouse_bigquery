with

failures as ( select * from {{ ref('failure_cases') }} )

,order_failure as (
    select
        order_id
        ,listagg(standard_category,' | ') within group (order by standard_category) as failure_reasons
    from failures
    where order_id is not null
    group by 1
)

select * from order_failure
