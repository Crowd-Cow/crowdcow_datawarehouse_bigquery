with

failures as ( select * from {{ ref('failure_cases') }} )

,order_failure as (
    select
        order_id
        ,STRING_AGG(standard_category, ' | ' ORDER BY standard_category) AS failure_reasons
    from failures
    where order_id is not null
    group by 1
)

select * from order_failure
