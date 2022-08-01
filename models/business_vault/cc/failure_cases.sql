with

failure_case as ( select * from {{ ref('stg_cc__failure_cases') }} )

select * from failure_case
